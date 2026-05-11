const {BigQuery, BigQueryDate} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

const getTable = async (tableName, isParent, siteCode) => {
    try {
        const dataset = bigquery.dataset('stats');
        const [tableData] = await dataset.table(tableName).getRows();
        let data = '';

        if(isParent) data = tableData.filter(dt => siteCode.indexOf(dt.siteCode) !== -1)
        else data = tableData.filter(dt => dt.siteCode === siteCode)
    
        return data;
    } catch (error) {
        if(/Not found: Table/i.test(error.message)) return [];
        else console.error(error)
    }
}

const stringToOperatorConvt = {
  equals: "=",
  notequals: "!=",
  greater: ">",
  greaterequals: ">=",
  less: "<",
  lessequals: "<=",
};

// Validate to a safe character set.
// concept IDs (optionally prefixed by `d_` after `convertToBigqueryKey`)
// And dotted nested paths like `state.uid`.
const SAFE_BQ_IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/;
const assertSafeBqIdentifier = (value, label) => {
  if (typeof value !== "string" || !SAFE_BQ_IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`Unsafe BigQuery identifier for ${label}: ${JSON.stringify(value)}`);
  }
  return value;
};

/**
 * Convert notification-spec filters into BigQuery WHERE-clause fragments and
 * a parameter map. Used by both recipient-count and recipient-fetch helpers.
 * @param {object} params
 * @param {Array<string|Array>} params.conditions Notification eligibility conditions.
 * @param {string} params.startTimeStr Upper-bound timestamp for the time field.
 * @param {string} params.stopTimeStr Lower-bound timestamp for the time field.
 * @param {string} params.timeField Firestore/BigQuery field used for time-window filtering.
 * @returns {{ fragments: string[], params: Record<string, unknown>, types: Record<string, string> }}
 */
const buildNotificationEligibilityConditions = ({
  conditions = [],
  startTimeStr = "",
  stopTimeStr = "",
  timeField = "",
}) => {
  const fragments = [];
  const params = {};
  const types = {};
  let nextParamIndex = 0;

  for (const condition of conditions) {
    if (typeof condition === "string") {
      // Pre-formed SQL fragments are trusted (they come from the admin-managed notificationSpec doc).
      fragments.push(`(${condition})`);
    } else if (Array.isArray(condition) && condition.length === 3) {
      const [key, operatorStr, value] = condition;
      const operator = stringToOperatorConvt[operatorStr];
      if (!operator) continue;

      const bqKey = assertSafeBqIdentifier(convertToBigqueryKey(key), "condition key");
      const paramName = `cond_${nextParamIndex++}`;
      params[paramName] = value;
      types[paramName] = typeof value === "number" ? "FLOAT64" : "STRING";
      fragments.push(`${bqKey} ${operator} @${paramName}`);
    }
  }

  if (timeField) {
    const bqTimeField = assertSafeBqIdentifier(convertToBigqueryKey(timeField), "time field");
    if (startTimeStr) {
      params.startTimeStr = startTimeStr;
      types.startTimeStr = "STRING";
      fragments.push(`${bqTimeField} < @startTimeStr`);
    }
    if (stopTimeStr) {
      params.stopTimeStr = stopTimeStr;
      types.stopTimeStr = "STRING";
      fragments.push(`${bqTimeField} >= @stopTimeStr`);
    }
  }

  return { fragments, params, types };
};

/**
 * Build the shared BigQuery SQL used by notification recipient fetches and
 * recipient counts. Count mode intentionally omits pagination clauses so both
 * helpers stay aligned on eligibility.
 * Returns a parameterized query payload that callers pass directly to `bigquery.query()`.
 * @param {object} params
 * @param {string} params.notificationSpecId Notification specification ID.
 * @param {string} params.selectSql SELECT clause used for non-count queries.
 * @param {Array<string|Array>} params.conditions Notification eligibility conditions.
 * @param {string} params.startTimeStr Upper-bound timestamp for the time field.
 * @param {string} params.stopTimeStr Lower-bound timestamp for the time field.
 * @param {string} params.timeField Firestore/BigQuery field used for time-window filtering.
 * @param {string} params.previousToken Token cursor for the next page.
 * @param {?number} params.limit Maximum row count for fetch queries.
 * @param {boolean} params.countOnly Whether to emit the COUNT(*) query variant.
 * @returns {{ query: string, params: Record<string, unknown>, types: Record<string, string> }}
 */
const buildNotificationEligibilityQuery = ({
  notificationSpecId = "",
  selectSql = "token",
  conditions = [],
  startTimeStr = "",
  stopTimeStr = "",
  timeField = "",
  previousToken = "",
  limit = null,
  countOnly = false,
}) => {
  const eligibility = buildNotificationEligibilityConditions({
    conditions,
    startTimeStr,
    stopTimeStr,
    timeField,
  });
  const fragments = [...eligibility.fragments];
  const params = { ...eligibility.params, notificationSpecId };
  const types = { ...eligibility.types, notificationSpecId: "STRING" };

  if (!countOnly && previousToken) {
    fragments.push(`token > @previousToken`);
    params.previousToken = previousToken;
    types.previousToken = "STRING";
  }

  // The LEFT JOIN identifies tokens that should NOT be re-fetched. A token
  // is excluded when any of the following is true (notifications spec doc)
  //   - isSent IS NULL or TRUE (legacy record or successfully sent)
  //   - processingState = 'send_failed' (permanent failure)
  // Records in `reserved`/`provider_send_in_flight`/`provider_acceptance_unknown` remain re-fetchable.
  // The per-record state machine handles re-reservation and duplicate-prevention safely.
  let query = `SELECT ${countOnly ? "COUNT(*) AS cnt" : selectSql}
    FROM \`Connect.participants\`
    LEFT JOIN (
      SELECT DISTINCT token, TRUE AS isSent
      FROM
        \`Connect.notifications\`
      WHERE
        notificationSpecificationsID = @notificationSpecId
        AND (IFNULL(isSent, TRUE) = TRUE OR processingState = 'send_failed'))
    USING (token)
    WHERE ${fragments.length === 0 ? "1=1" : fragments.join(" AND ")}
    AND isSent IS NULL`;

  if (!countOnly) {
    const numericLimit = Number(limit);
    if (!Number.isFinite(numericLimit) || numericLimit <= 0 || !Number.isInteger(numericLimit)) {
      throw new Error(`Unsafe BigQuery limit: ${JSON.stringify(limit)}`);
    }
    query += ` ORDER BY token LIMIT ${numericLimit}`;
  }

  return { query, params, types };
};

/**
 * Fetch one token-ordered page of notification-eligible participants from BigQuery.
 * The query excludes recipients already present in `Connect.notifications` for the
 * same notification spec.
 * @param {object} params
 * @param {string} params.notificationSpecId Notification specification ID.
 * @param {Array<string|Array>} params.conditions Notification eligibility conditions.
 * @param {string} params.startTimeStr Upper-bound timestamp for the time field.
 * @param {string} params.stopTimeStr Lower-bound timestamp for the time field.
 * @param {string} params.timeField Firestore/BigQuery field used for time-window filtering.
 * @param {string[]} params.fieldsToFetch Fields to select from BigQuery.
 * @param {number} params.limit Maximum number of rows to return.
 * @param {string} params.previousToken Token cursor for the next page.
 * @returns {Promise<object[]>} Firestore-shaped participant rows.
 */
async function getParticipantsForNotificationsBQ({
  notificationSpecId = "",
  conditions = [],
  startTimeStr = "",
  stopTimeStr = "",
  timeField = "",
  fieldsToFetch = [],
  limit = 100,
  previousToken = "",
}) {
  if (!notificationSpecId || !Array.isArray(conditions) || conditions.length === 0) return [];

  const bqFieldArray = fieldsToFetch
    .map(convertToBigqueryKey)
    .map((field) => `${assertSafeBqIdentifier(field, "select field")} AS ${field.replace(/\./g, "_DOT_")}`);
  const { query, params, types } = buildNotificationEligibilityQuery({
    notificationSpecId,
    selectSql: bqFieldArray.length === 0 ? "token" : bqFieldArray.join(", "),
    conditions,
    startTimeStr,
    stopTimeStr,
    timeField,
    previousToken,
    limit,
  });
  const [rows] = await bigquery.query({ query, params, types });
  if (rows.length === 0) return [];

  return rows.map(convertToFirestoreData);
}

/**
 * Count notification-eligible participants using the same eligibility query as
 * `getParticipantsForNotificationsBQ()`, but without pagination clauses.
 * Returns three distinct sentinel values so callers can distinguish:
 *   -  0  : query ran successfully, no recipients matched
 *   - >0  : query ran successfully, this many recipients matched
 *   - -1  : query failed (network, BigQuery error, etc.)
 *   - -2  : spec is misconfigured (missing id or empty conditions). Query never ran
 * @param {object} params
 * @param {string} params.notificationSpecId Notification specification ID.
 * @param {Array<string|Array>} params.conditions Notification eligibility conditions.
 * @param {string} params.startTimeStr Upper-bound timestamp for the time field.
 * @param {string} params.stopTimeStr Lower-bound timestamp for the time field.
 * @param {string} params.timeField Firestore/BigQuery field used for time-window filtering.
 * @returns {Promise<number>} See sentinel meanings above.
 */
async function countParticipantsForNotificationsBQ({
  notificationSpecId = "",
  conditions = [],
  startTimeStr = "",
  stopTimeStr = "",
  timeField = "",
}) {
  if (!notificationSpecId || !Array.isArray(conditions) || conditions.length === 0) return -2;
  const { query, params, types } = buildNotificationEligibilityQuery({
    notificationSpecId,
    conditions,
    startTimeStr,
    stopTimeStr,
    timeField,
    countOnly: true,
  });
  try {
    const [rows] = await bigquery.query({ query, params, types });
    return rows.length > 0 ? Number(rows[0].cnt) : 0;
  } catch (error) {
    console.error("Error in countParticipantsForNotificationsBQ().", error);
    return -1; // Caller treats -1 as "query failed"
  }
}

async function getParticipantsForRequestAKitBQ(conditions = [], sorts = [], limit, selectFields = []) {
  let bqConditionArray = [];
  let bqSortArray = [];


  for (const condition of conditions) {
    if (typeof condition === "string") {
      bqConditionArray.push(`(${condition})`);
    } else if (Array.isArray(condition) && condition.length === 3) {
      const [key, operatorStr, value] = condition;
      const operator = stringToOperatorConvt[operatorStr];
      if (!operator) continue;

      const bqKey = convertToBigqueryKey(key);
      bqConditionArray.push(`${bqKey} ${operator} ${typeof value === "number" ? value : `"${value}"`}`);
    }
  }

  for (const sort of sorts) {
    const [key, sortOrder] = sort;
    const bqKey = convertToBigqueryKey(key);
    bqSortArray.push(`${bqKey}${sortOrder ? ` ${sortOrder}` : ''}`)
  }

  const queryStr = `SELECT Connect_ID, token${selectFields.length ? ` ${selectFields.join(', ')}` : ''} FROM \`Connect.participants\` 
  ${bqConditionArray.length ? `WHERE ${bqConditionArray.join(" AND ")}` : ''} 
  ORDER BY ${bqSortArray.length ? bqSortArray.join(', ') : `token`} ${limit ? `LIMIT ${limit}` : ''}
  `;

  try {
    const [rows] = await bigquery.query(queryStr);

    return {queryStr, rows: rows.map(convertToFirestoreData)};
  } catch(err) {
    // There are reasonable odds this will happen due to user error in
    // building SQL conditions, so show them the full query for debugging
    console.error(err);
    throw new Error(`Error processing query string ${queryStr}: ${err?.message || err}`)
  }
  
}

/**
 * Unflatten and convert to firestore data format
 * @param {object} bqData data from BQ
 * @returns
 */
function convertToFirestoreData(bqData) {
  let result = {};
  let keySet = new Set();

  for (const [bqKey, val] of Object.entries(bqData)) {
    if (val === null) continue;
    const longKey = convertToFirestoreKey(bqKey).replace(/_DOT_/g, ".");
    if (!longKey.includes(".")) {
      if (typeof val === "object" && !Array.isArray(val)) {
        result[longKey] = convertToFirestoreData(val);
        continue;
      }

      result[longKey] = val;
      continue;
    }

    const [currKey, ...restKeys] = longKey.split(".");
    keySet.add(currKey);
    result[currKey] = result[currKey] || {};
    result[currKey][restKeys.join(".")] = val;
  }

  for (const key of keySet) {
    result[key] = convertToFirestoreData(result[key]);
  }

  return result;
}

function convertToBigqueryKey(str) {
  return str.replace(/(?<=^|\.)(\d)/g, "d_$1");
}

function convertToFirestoreKey(str) {
  return str.replace(/(?<=^|\.)d_(\d)/g, "$1");
}

/**
 * @param {string} tableName
 * @param {number | number[]} siteCode
 */
const getStatsFromBQ = async (tableName, siteCode) => {
  const query = `SELECT * FROM \`stats.${tableName}\` WHERE siteCode IN UNNEST(@siteCode)`;
  const options = {
    query,
    location: "US",
    params: { siteCode: Array.isArray(siteCode) ? siteCode : [siteCode] },
  };
  
  let rows = [];
  try {
    [rows] = await bigquery.query(options);
  } catch (error) {
    console.error("getStatsFromBQ() error.", error);
  }
  
  return rows;
};

/**
 * Gets the collection stats for sites
 * @param {number | number[]} siteCode
 */
const getCollectionStats = async (type, siteCode) => {
  let  query = `
    WITH actual_dups AS (
      SELECT *
      FROM FlatConnect.biospecimen_JP
      WHERE d_556788178 IS NOT NULL AND d_410912345 is not null and Connect_ID is not null
      QUALIFY COUNT(*) OVER (PARTITION BY Connect_ID) > 1
    ),

    rows_to_keep AS (
      SELECT *
      FROM actual_dups
      QUALIFY d_556788178 = MIN(d_556788178) OVER (PARTITION BY Connect_ID)
    ),

    non_duplicate_rows AS (
      SELECT orig.*
      FROM FlatConnect.biospecimen_JP AS orig
      LEFT JOIN actual_dups AS dups
      ON orig.Connect_ID = dups.Connect_ID
      WHERE dups.Connect_ID IS NULL
    ),

    no_dups AS (
      SELECT * FROM rows_to_keep
      UNION ALL
      SELECT * FROM non_duplicate_rows
    )

    SELECT 
      p.d_827220437 AS siteCode,
      count(*) as verfiedPts
    FROM FlatConnect.participants_JP AS p
    LEFT JOIN no_dups AS n
      ON p.Connect_ID = n.Connect_ID
    WHERE 
      p.Connect_ID IS NOT NULL
      AND p.d_821247024 = '197316935'
      AND (p.d_512820379 = '486306141' OR p.d_512820379 = '854703046')
      AND p.d_831041022 = '104430631' AND
      (d_878865966 = '353358909' OR d_167958071 = '353358909' OR
      d_684635302 = '353358909') AND
      n.d_556788178 IS NOT NULL AND n.d_410912345 IS NOT NULL AND 
      p.d_827220437 IN UNNEST(@siteCode)`;

  switch (type) {
    case "research":
      query += `
       AND ((d_650516960 = '534621077' OR d_173836415_d_266600170_d_592099155 = '534621077' OR
        d_173836415_d_266600170_d_718172863 = '534621077' OR d_173836415_d_266600170_d_915179629 = '534621077'))`;
      break;
    case "clinical":
      query += `
        AND ((d_650516960 = '664882224' OR d_173836415_d_266600170_d_592099155 = '664882224' OR
        d_173836415_d_266600170_d_718172863 = '664882224' OR d_173836415_d_266600170_d_915179629 = '664882224'))`;
      break;
    case "all":
    default:
      //No additional filters needed
      break;
  }
  query += ' GROUP BY siteCode';

  //The siteCodes need to be converted to strings because of the data type in bigquery
  const options = {
    query,
    location: "US",
    params: { siteCode: Array.isArray(siteCode) ? siteCode.map((val) => {return ''+val;}) : [''+siteCode] }
  };

  let rows = [];
  try {
    [rows] = await bigquery.query(options);
  } catch (error) {
    console.error("getCollectionStats() error.", error);
  }

  //The siteCodes need to be converted to integers to keep the data consistent with the other stats
  return rows.map((row) => {
      row.siteCode = parseInt(row.siteCode, 10)
      return row;
  });
}

/**
 * 
 * @param {string} fullNumber Phone number in the format +11234567890
 * @returns {Promise<string[]>} Array of tokens of participant(s) having the phone number
 */
const getParticipantTokensByPhoneNumber = async (fullNumber) => {
  const tenDigitsNumber = fullNumber.slice(-10);
  const query = `SELECT token FROM \`Connect.participants\` WHERE d_348474836 = @fullNumber OR d_388711124 = @tenDigitsNumber`;
  const options = {
    query,
    location: "US",
    params: { fullNumber, tenDigitsNumber },
  };
  
  let rows = [];
  try {
    [rows] = await bigquery.query(options);
  } catch (error) {
    console.error("Error calling getParticipantTokensByPhoneNumber().", error);
  }
  
  return rows.map(row => row.token);
};

/**
 * getColumnsFromTable - retrieves column details from a table or view
 * 
 * @param string dataset
 * @param string table
 * @returns Object[]
 */
async function getColumnsFromTable (dataset, table) {
  //@todo
  //Determine if the view_birthday_card view needs to become a real view
  if (dataset === 'NORC' && table === 'view_birthday_card') {
    //Hard Coded "view"
    return [
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "CONNECT_ID", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "PIN", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "token", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "DOBM", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "birth_month", "data_type": "INT64"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "first_name", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "last_name", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "address_line_1", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "address_line_2", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "city", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "state", "data_type": "STRING"},
      {"table_schema": "NORC", "table_name": "view_birthday_card", "column_name": "zip_code", "data_type": "STRING"},
    ];
  } else {
    //Lookup the column information from the information_schema
    try {
      const queryStr = `SELECT * EXCEPT(is_generated, generation_expression, is_stored, is_updatable)
          FROM \`${dataset}.INFORMATION_SCHEMA.COLUMNS\` 
          WHERE table_name = '${table}'`;
      const [rows] = await bigquery.query(queryStr);
      if (rows.length === 0) return [];
      return rows;
    } catch (e) {
        console.error(e);
        return null;
    }
  }
}

/**
 * validateFilters - Validates the filters for a table or view to make sure the fields are valid
 * 
 * @param {string} dataset 
 * @param {string} table 
 * @param {Object[]} filters 
 */
async function validateFilters (dataset, table, filters) {
  let fields = await getColumnsFromTable(dataset, table);
  if (Array.isArray(fields) && fields.length > 0) {
    let isValid = true;
    if (Array.isArray(filters)) {
      filters.forEach(filter => {
        if (!filter.column || !filter.operator) {
          isValid = false;
        }
        //If the field is not in the table or view then it is invalid
        let fieldIndex = fields.findIndex((field) => field.column_name === filter.column);
        if (fieldIndex === -1) {
          isValid = false;
        }
        //@todo Determine if we should use the data_type of the field to validate operators or values
      })
    }
    return isValid;
  } else {
    return false;
  }

}

/**
 * validateFilters - Validates the filters for a table or view to make sure the fields are valid
 * 
 * @param {string} dataset 
 * @param {string} table 
 * @param {String[]} fields 
 */
async function validateFields (dataset, table, fieldsToCheck) {
  let fields = await getColumnsFromTable(dataset, table);
  if (Array.isArray(fields) && fields.length > 0) {
    let isValid = true;
    if (Array.isArray(fieldsToCheck)) {
      fieldsToCheck.forEach(fieldToCheck => {
        //If the field is not in the table or view then it is invalid
        let fieldIndex = fields.findIndex((field) => {
          return field.column_name === fieldToCheck
        });
        if (fieldIndex === -1) {
          isValid = false;
        }
      })
    }
    return isValid;
  } else {
    return false;
  }

}

/**
 * validateTableAccess - validates access to a table or view by checking BigQuery IAM permissions
 * 
 * First verifies the dataset and table exist, then checks if the service account email
 * has been granted BigQuery Data Viewer access on the table.
 * 
 * @param {Object} authObj - Authorization object containing saEmail from siteDetails
 * @param {string} dataset - The BigQuery dataset name
 * @param {string} table - The BigQuery table or view name
 * @return {Promise<boolean>} - True if access is granted, false otherwise
 */
async function validateTableAccess (authObj, dataset, table) {
  if (!authObj?.saEmail) {
    console.error("validateTableAccess: No service account email provided");
    return false;
  }

  const saEmail = authObj.saEmail;

  try {
    // Step 1: Verify the dataset exists
    const datasetRef = bigquery.dataset(dataset);
    const [datasetExists] = await datasetRef.exists();
    if (!datasetExists) {
      console.error(`validateTableAccess: Dataset '${dataset}' does not exist`);
      return false;
    }

    // Step 2: Verify the table exists
    const tableRef = datasetRef.table(table);
    const [tableExists] = await tableRef.exists();
    if (!tableExists) {
      console.error(`validateTableAccess: Table '${dataset}.${table}' does not exist`);
      return false;
    }

    // Step 3: Check table-level IAM policy for viewer access
    const [tablePolicy] = await tableRef.getIamPolicy();
    if (hasViewerAccess(tablePolicy, saEmail)) {
      return true;
    }

    console.log(`validateTableAccess: ${saEmail} does not have viewer access to ${dataset}.${table}`);
    return false;

  } catch (error) {
    console.error(`validateTableAccess: Error checking access for ${saEmail} on ${dataset}.${table}:`, error);
    return false;
  }
}

/**
 * Helper function to check if a service account email has BigQuery viewer access in an IAM policy
 * 
 * @param {Object} policy - IAM policy object with bindings array
 * @param {string} saEmail - Service account email to check
 * @return {boolean} - True if the email has viewer (or higher) access
 */
function hasViewerAccess(policy, saEmail) {
  if (!policy?.bindings || !Array.isArray(policy.bindings)) {
    console.log('hasViewerAccess: No bindings found in policy or bindings is not an array');
    return false;
  }

  const viewerRoles = [
    'roles/bigquery.dataViewer',
  ];

  const memberFormats = [
    `serviceAccount:${saEmail.toLowerCase()}`,
  ];

  for (const binding of policy.bindings) {
    if (!viewerRoles.includes(binding.role)) {
      continue;
    }
    
    if (binding.members && Array.isArray(binding.members)) {
      for (const member of binding.members) {
        if (memberFormats.includes(member)) {
          return true;
        }
      }
    }
  }

  return false;
}

function getQueryPartsForTable (dataset, table) {  
  return {
    "SELECT": "*",
    "FROM": dataset+'.'+table
  };
}

/**
 * Return Big query Data for a given dataset, table, filter, and fields
 * 
 * 
 * @param {string} dataset 
 * @param {string} table 
 * @param {Object[]} filters 
 * @param {String[]} fields 
 * @returns 
 */
async function getBigQueryData(dataset, table, filters, fields) {
  const queryParts =  getQueryPartsForTable(dataset, table);
  let queryStr = '';
  if (queryParts.WITH) {
    queryStr += 'WITH '+queryParts.WITH + ' ';
  }
  queryStr += 'SELECT ';
  if (Array.isArray(fields)) {
    queryStr += fields.join(', ');
  } else if (queryParts.SELECT) {
    queryStr += queryParts.SELECT;
  } else {
    queryStr += '*'
  }

  queryStr += ' FROM '+queryParts.FROM;
  let filterValues = [];
  if (queryParts.WHERE || (Array.isArray(filters) && filters.length > 0)) {
    queryStr += ' WHERE ';
  }
  if (queryParts.WHERE) {
    queryStr += queryParts.WHERE;
  }
  if ((Array.isArray(filters) && filters.length > 0)) {
    if (queryParts.WHERE) {
      queryStr += ' AND (';
    }
    filters.forEach((filter, index) => {
      if (index > 0) {
        queryStr += ' AND ';
      }
      queryStr += filter.column + ' ' + filter.operator;
      if (filter.value) {
        queryStr += ' ?';
        filterValues.push(filter.value);
      }
    });
    if (queryParts.WHERE) {
      queryStr += ')';
    }
  }

  let queryObj = {
    query: queryStr,
  };
  if (Array.isArray(filterValues) && filterValues.length > 0) {
    queryObj.params = filterValues;
  }
  const [rows] = await bigquery.query(queryObj);
  if (rows.length === 0) return [];
  //Convert any BigQuery Objects
  rows.forEach((row, rowIndex) => {
    Object.keys(row).forEach(key => {
      if (row[key] instanceof BigQueryDate) {
        rows[rowIndex][key] = row[key].value;
      }
    })
  })
  return rows;
}

const getPhysicalActivityData = async (expression) => {
  const query = `
    SELECT *
    FROM \`${process.env.GCLOUD_PROJECT}.ROI.physical_activity\`
    WHERE DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', d_416831581)) = ${expression}
  `;

  const [rows] = await bigquery.query(query);
  return rows;
}

module.exports = {
    getTable,
    getParticipantsForNotificationsBQ,
    countParticipantsForNotificationsBQ,
    getParticipantsForRequestAKitBQ,
    getStatsFromBQ,
    getCollectionStats,
    getParticipantTokensByPhoneNumber,
    validateFields,
    validateFilters,
    validateTableAccess,
    getBigQueryData,
    getPhysicalActivityData
};

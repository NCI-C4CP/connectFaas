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

/**
 * Get participant data fields from BigQuery, based on conditions.
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
    .map((field) => `${field} AS ${field.replace(/\./g, "_DOT_")}`);
  let bqConditionArray = [];

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

  if (timeField) {
    const bqTimeField = convertToBigqueryKey(timeField);
    if (startTimeStr) bqConditionArray.push(`${bqTimeField} <= "${startTimeStr}"`);
    if (stopTimeStr) bqConditionArray.push(`${bqTimeField} >= "${stopTimeStr}"`);
  }

  const queryStr = `SELECT ${bqFieldArray.length === 0 ? "token" : bqFieldArray.join(", ")}
    FROM \`Connect.participants\` 
    LEFT JOIN (
      SELECT DISTINCT token, TRUE AS isSent
      FROM
        \`Connect.notifications\`
      WHERE
        notificationSpecificationsID = "${notificationSpecId}")
    USING (token)
    WHERE ${bqConditionArray.length === 0 ? "1=1" : bqConditionArray.join(" AND ")}
    AND isSent IS NULL AND token > "${previousToken}" ORDER BY token LIMIT ${limit}`;
  const [rows] = await bigquery.query(queryStr);
  if (rows.length === 0) return [];

  return rows.map(convertToFirestoreData);
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
 * validateTableAccess - validates access to a table or view
 * 
 * @param {Object} authObj 
 * @param {string} dataset 
 * @param {string} table 
 * @return {boolean}
 */
async function validateTableAccess (authObj, dataset, table) {
  /**
   * TODO: Implement this via IAM.  It needs to be determined if the IAM can grant access to certain tables and columns or if
   * the granularity is too broad and we need to only allow access to views to limit the field access.  
   * 
   * This function is currently async because it is anticpated that it will need to lookup data from IAM and the signature 
   * for the function shouldn't change
   */
  let allowAccess = false;
  switch (dataset) {
    case "NORC":
      switch (table) {
        case "view_birthday_card":
          if (["NIH", "NORC"].includes(authObj.acronym)) {
            allowAccess = true;
          }
          break;
        case "view_phone_prompting_details":
          if (["NIH", "NORC"].includes(authObj.acronym)) {
            allowAccess = true;
          }
          break;
      }
  }
  return allowAccess;
}

function getQueryPartsForTable (dataset, table) {
  if (dataset === 'NORC' && table === 'view_birthday_card') {
    return {
      "WITH": 'AddressInfo AS (SELECT  Connect_ID,  token,  CAST(d_564964481 AS INT64) AS birth_month,  d_996038075 AS last_name,  d_399159511 AS first_name,  d_521824358 AS address_line_1,  d_442166669 AS address_line_2,  d_703385619 AS city,  d_634434746 AS state,  d_892050548 AS zip_code,  d_821247024,   d_747006172,   d_987563196,   d_827220437 FROM `FlatConnect.participants_JP`)',
      "SELECT": "Connect_ID AS CONNECT_ID,  Connect_ID AS PIN,  token,  LPAD(CAST(birth_month AS STRING), 2, '0') AS DOBM, first_name,  last_name,  address_line_1,  address_line_2,  city,  state,  zip_code",
      "FROM": "AddressInfo",
      "WHERE": "d_821247024 = '197316935' AND d_747006172 != '353358909' AND d_987563196 != '353358909'"
    };
  } else {
    return {
      "SELECT": "*",
      "FROM": dataset+'.'+table
    };
  }
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
    getStatsFromBQ,
    getCollectionStats,
    getParticipantTokensByPhoneNumber,
    validateFields,
    validateFilters,
    validateTableAccess,
    getBigQueryData,
    getPhysicalActivityData
};

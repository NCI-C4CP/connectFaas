const { v4: uuid } = require("uuid");
const sgMail = require("@sendgrid/mail");
const showdown = require("showdown");
const twilio = require("twilio");
const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const {getResponseJSON, setHeadersDomainRestricted, setHeaders, logIPAddress, redactEmailLoginInfo, redactPhoneLoginInfo, validEmailFormat, getTemplateForEmailLink, nihMailbox, getSecret, cidToLangMapper, unsubscribeTextObj, getAdjustedTime, delay} = require("./shared");
const {getNotificationSpecById, getNotificationSpecByCategoryAndAttempt, getNotificationSpecsByScheduleOncePerDay, saveNotificationBatch, generateSignInWithEmailLink, storeNotification, checkIsNotificationSent, updateSmsPermission} = require("./firestore");
const {getParticipantsForNotificationsBQ} = require("./bigquery");
const conceptIds = require("./fieldToConceptIdMapping");

const converter = new showdown.Converter();
const langArray = ["english", "spanish"];
let twilioClient, messagingServiceSid;
let isSendGridSetup = false;
let isTwilioSetup = false;
let isSendingNotifications = false; // A more robust soluttion is needed when using multiple servers 

const setupSendGrid = async () => {
  if (isSendGridSetup) return;
  const apiKey = await getSecret(process.env.GCLOUD_SENDGRID_SECRET);
  sgMail.setApiKey(apiKey);
  isSendGridSetup = true;
};

const setupTwilio = async () => {
  const secretsToFetch = {
    accountSid: process.env.TWILIO_ACCOUNT_SID,
    authToken: process.env.TWILIO_AUTH_TOKEN,
    messagingServiceSid: process.env.TWILIO_MESSAGING_SERVICE_SID
  };
  const client = new SecretManagerServiceClient();
  let fetchedSecrets = {};
  for (const [key, value] of Object.entries(secretsToFetch)) {
    const [version] = await client.accessSecretVersion({ name: value });
    fetchedSecrets[key] = version.payload.data.toString();
  }

  twilioClient = twilio(fetchedSecrets.accountSid, fetchedSecrets.authToken);
  messagingServiceSid = fetchedSecrets.messagingServiceSid;
  isTwilioSetup = true;
};

/**
 * Send Twilio SMS message using API.
 * Set up Twilio client and messaging service SID before calling this function.
 * @param {Object} smsRecord SMS record object to be saved to Firestore
 * @returns {Promise<Object>}
 */
const sendTwilioMessage = async (smsRecord) => {
  try {
    const result = await twilioClient.messages.create({
      body: smsRecord.notification.body,
      to: smsRecord.phone,
      messagingServiceSid,
    });
    const updatedSmsRecord = { ...smsRecord, messageSid: result.sid || "" };
    return { smsRecord: updatedSmsRecord, isSuccess: true, isRateLimit: false };
  } catch (error) {
    const errorCode = error.code?.toString() ?? "";
    const statusCode = (error.status ?? error.statusCode)?.toString() ?? "";
    if (errorCode === "20429" || statusCode === "429") {
      return { smsRecord, isSuccess: false, isRateLimit: true };
    }

    console.error(
      `Error sending SMS (participant token: ${smsRecord.token}; spec ID: ${smsRecord.notificationSpecificationsID}).`,
      error,
    );
    return { smsRecord, isSuccess: false, isRateLimit: false };
  }
};

/**
 * Handles rate-limited batch sending of Twilio SMS messages with retry logic.
 * Possible improvements: When having more than 1 phone number sending out messages, create a  separate queue for each phone number to increase overall throughput.
 */
class SmsBatchSender {
  #queue = [];
  #isProcessing = false;
  #sentCounts = {}; // { [specId]: { english: number, spanish: number } }
  #failedCounts = {}; // { [specId]: { english: number, spanish: number } }
  #retryCounts = {}; // { [specId]: { [recordId]: number } }
  #finishedSpecSet = new Set();
  #batchSize;
  #maxRetries;
  #prevBatchFinishTime = 0;
  #prevProgressLogTime = Date.now();
  #sendFn;
  #saveFn;
  #delayFn;

  constructor({
    batchSize = 150,
    maxRetries = 5,
    sendFn = sendTwilioMessage,
    saveFn = saveNotificationBatch,
    delayFn = delay,
  } = {}) {
    this.#batchSize = batchSize;
    this.#maxRetries = maxRetries;
    this.#sendFn = sendFn;
    this.#saveFn = saveFn;
    this.#delayFn = delayFn;
  }

  /**
   * Add multiple SMS records to the queue and trigger processing.
   * Each record must have notificationSpecificationsID and language properties.
   * @param {Object[]} smsRecords - Array of SMS record objects
   */
  addToQueue(smsRecords) {
    this.#queue.push(...smsRecords);
    this.#processQueue();
  }

  /**
   * Get counts of successfully sent SMS messages for a specific spec ID.
   * @param {string} specId - Notification specification ID
   * @returns {Object} Counts object: { english: number, spanish: number }
   */
  getSentCounts(specId) {
    return this.#sentCounts[specId] ? { ...this.#sentCounts[specId] } : { english: 0, spanish: 0 };
  }

  /**
   * Get counts of failed SMS messages for a specific spec ID.
   * @param {string} specId - Notification specification ID
   * @returns {Object} Counts object: { english: number, spanish: number }
   */
  getFailedCounts(specId) {
    return this.#failedCounts[specId] ? { ...this.#failedCounts[specId] } : { english: 0, spanish: 0 };
  }

  /**
   * Mark that all SMS messages for a spec have been added to the queue.
   * Call this after adding all messages for a spec to signal completion.
   * @param {string} specId - Notification specification ID
   */
  markSpecEnd(specId) {
    this.#queue.push({ specId, isEndMarker: true });
    this.#processQueue();
  }

  /**
   * Check if all SMS messages for a spec have been processed (sent or failed).
   * @param {string} specId - Notification specification ID
   * @returns {boolean} True if the spec's end marker has been processed
   */
  isSpecFinished(specId) {
    return this.#finishedSpecSet.has(specId);
  }

  /**
   * Wait for all SMS messages for a spec to be processed.
   * @param {string} specId - Notification specification ID
   * @param {number} [checkIntervalMs=1000] - Interval between checks in milliseconds
   * @returns {Promise<{sentCounts: {english: number, spanish: number}, failedCounts: {english: number, spanish: number}}>}
   */
  async waitForSpec(specId, checkIntervalMs = 1000) {
    while (!this.isSpecFinished(specId)) {
      await this.#delayFn(checkIntervalMs);
    }
    return {
      sentCounts: this.getSentCounts(specId),
      failedCounts: this.getFailedCounts(specId),
    };
  }

  /**
   * Increment the sent or failed count for a given spec ID and language.
   * @param {Object} countsObj - The counts object to update (this.#sentCounts or this.#failedCounts)
   * @param {string} specId - Notification specification ID
   * @param {string} language - Language key ("english" or "spanish")
   */
  #incrementCount(countsObj, specId, language) {
    if (!countsObj[specId]) {
      countsObj[specId] = { english: 0, spanish: 0 };
    }
    countsObj[specId][language]++;
  }

  /**
   * Log sent and failed counts for all in-progress specs. Throttled to at most once every 30 seconds.
   */
  #logProgress() {
    const now = Date.now();
    if (now - this.#prevProgressLogTime < 30_000) return;
    this.#prevProgressLogTime = now;

    const specIds = new Set([...Object.keys(this.#sentCounts), ...Object.keys(this.#failedCounts)]);
    for (const specId of specIds) {
      if (this.#finishedSpecSet.has(specId)) continue;
      const sent = this.#sentCounts[specId] ?? { english: 0, spanish: 0 };
      const failed = this.#failedCounts[specId] ?? { english: 0, spanish: 0 };
      console.log(
        `SMS in progress (spec ID ${specId}): sent ${sent.english + sent.spanish} (en: ${sent.english}, es: ${sent.spanish}), ` +
          `failed ${failed.english + failed.spanish} (en: ${failed.english}, es: ${failed.spanish}).`,
      );
    }
  }

  /**
   * Process the SMS queue in batches with rate-limit handling.
   * Sends batches of SMS messages, retries rate-limited messages (up to maxRetries),
   * saves successful records to Firestore, and marks specs as finished when their end markers are reached.
   * Only one instance runs at a time; subsequent calls are no-ops while processing is active.
   * @returns {Promise<void>}
   */
  async #processQueue() {
    if (this.#isProcessing) return;
    this.#isProcessing = true;
    const delayTimeMs = 1000;

    while (this.#queue.length > 0) {
      const elapsedTime = Date.now() - this.#prevBatchFinishTime;
      if (elapsedTime < delayTimeMs) {
        await this.#delayFn(delayTimeMs - elapsedTime);
      }

      const batchItems = this.#queue.splice(0, this.#batchSize);
      const endMarkerSpecIdSet = new Set(batchItems.filter((item) => item.isEndMarker).map((item) => item.specId));
      const batchSmsRecords = batchItems.filter((item) => !item.isEndMarker);

      if (batchSmsRecords.length > 0) {
        const batchSendResults = await Promise.all(batchSmsRecords.map((r) => this.#sendFn(r)));
        this.#prevBatchFinishTime = Date.now();

        const successIndices = batchSendResults
          .map((res, idx) => (res.isSuccess ? idx : -1))
          .filter((idx) => idx !== -1);
        const successRecords = successIndices.map((idx) => batchSendResults[idx].smsRecord);

        if (successRecords.length > 0) {
          try {
            await this.#saveFn(successRecords);
            for (const idx of successIndices) {
              this.#incrementCount(
                this.#sentCounts,
                batchSmsRecords[idx].notificationSpecificationsID,
                batchSmsRecords[idx].language,
              );
            }
          } catch (error) {
            console.error("Error running saveNotificationBatch.", error);
          }
        }

        const rateLimitIndices = batchSendResults
          .map((res, idx) => (!res.isSuccess && res.isRateLimit ? idx : -1))
          .filter((idx) => idx !== -1)
          .reverse();
        for (const idx of rateLimitIndices) {
          const record = batchSmsRecords[idx];
          const specId = record.notificationSpecificationsID;
          if (!this.#retryCounts[specId]) this.#retryCounts[specId] = {};
          this.#retryCounts[specId][record.id] = (this.#retryCounts[specId][record.id] || 0) + 1;

          if (this.#retryCounts[specId][record.id] > this.#maxRetries) {
            console.error(
              `SMS rate limit retries exhausted (token: ${record.token}; spec ID: ${record.notificationSpecificationsID}).`,
            );
            this.#incrementCount(this.#failedCounts, record.notificationSpecificationsID, record.language);
            continue;
          }

          const currItems = [record];
          if (endMarkerSpecIdSet.has(specId)) {
            endMarkerSpecIdSet.delete(specId);
            currItems.push({ specId, isEndMarker: true });
          }
          this.#queue.unshift(...currItems);
        }

        const failedIndices = batchSendResults
          .map((res, idx) => (!res.isSuccess && !res.isRateLimit ? idx : -1))
          .filter((idx) => idx !== -1);
        for (const idx of failedIndices) {
          this.#incrementCount(
            this.#failedCounts,
            batchSmsRecords[idx].notificationSpecificationsID,
            batchSmsRecords[idx].language,
          );
        }
      }

      for (const specId of endMarkerSpecIdSet) {
        this.#finishedSpecSet.add(specId);
        delete this.#retryCounts[specId];
      }

      this.#logProgress();
    }
    this.#isProcessing = false;
  }
}

const smsBatchSender = new SmsBatchSender({});

const subscribeToNotification = async (req, res) => {
    setHeadersDomainRestricted(req, res);

    if(req.method === 'OPTIONS') return res.status(200).json({code: 200});

    if(req.method !== 'POST') {
        return res.status(405).json(getResponseJSON('Only POST requests are accepted!', 405));
    }

    if(!req.headers.authorization || req.headers.authorization.trim() === ""){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }

    const idToken = req.headers.authorization.replace('Bearer','').trim();
    const { validateIDToken } = require('./firestore');
    const decodedToken = await validateIDToken(idToken);

    if(decodedToken instanceof Error){
        return res.status(401).json(getResponseJSON(decodedToken.message, 401));
    }

    if(!decodedToken){
        return res.status(401).json(getResponseJSON('Authorization failed!', 401));
    }
    const data = req.body;
    console.log(decodedToken.uid , JSON.stringify(data));
    if(Object.keys(data).length <= 0 && data.token === undefined){
        return res.status(400).json(getResponseJSON('Bad request!', 400));
    }
    const notificationToken = data.token;

    const { notificationTokenExists } = require('./firestore');
    const { storeNotificationTokens } = require('./firestore');
    const uid = await notificationTokenExists(notificationToken);
    if(uid && uid !== decodedToken.uid) return res.status(403).json(getResponseJSON('Token is already associated with another user', 403))
    if(uid) return res.status(400).json(getResponseJSON('Token already exists', 400));
    storeNotificationTokens({notificationToken, uid: decodedToken.uid})
    res.status(200).json({message: 'Success!', code:200})
}

const markAllNotificationsAsAlreadyRead = (notification, collection) => {
    for(let id of notification) {
        if(id) {
            const {markNotificationAsRead} = require('./firestore');
            markNotificationAsRead(id, collection);
        }
    }
}

const retrieveNotifications = async (req, res, uid) => {
  if (req.method !== "GET") {
    return res.status(405).json(getResponseJSON("Only GET requests are accepted!", 405));
  }

  const { retrieveUserNotifications } = require("./firestore");
  try {
    const notificationArray = await retrieveUserNotifications(uid);
    if (notificationArray.length > 0 && req.query.markasread === 'true') {
      markAllNotificationsAsAlreadyRead(
        notificationArray.map((notification) => notification.id),
        "notifications"
      );
    }
    return res.status(200).json({ data: notificationArray, message: "Success", code: 200 });
  } catch (error) {
    console.error("Error when retrieving notifications.", error);
    return res.status(500).json({ data: [], message: "Internal Server Error", code: 500 });
  }
};

const sendEmail = async (emailTo, messageSubject, html, cc) => {
    await setupSendGrid();
    const msg = {
        to: emailTo,
        from: {
            name: process.env.SG_FROM_NAME || 'Connect for Cancer Prevention Study',
            email: process.env.SG_FROM_EMAIL || 'no-reply-myconnect@mail.nih.gov'
        },
        subject: messageSubject,
        html: html,
    };
    if(cc) msg.cc = cc;
    sgMail.send(msg).then(() => {
        console.log('Email sent to '+emailTo)
    })
    .catch((error) => {
        console.error(error)
    });
}

/**
 * Notifications handler triggered by an HTTP request from cloud scheduler.
 * @param {Request} req HTTP request
 * @param {Response} res HTTP response
 */
async function sendScheduledNotifications(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json(getResponseJSON("Only POST requests are accepted!", 405));
  }

  if (isSendingNotifications) {
    console.log("Function sendScheduledNotifications() is already running. Exiting...");
    return res.status(208).json(getResponseJSON("Function is already running.", 208));
  }

  if (!req.body || !req.body.scheduleAt) {
    return res.status(400).json(getResponseJSON("Field scheduleAt is missing in request body.", 400));
  }

  isSendingNotifications = true;
  try {
    const notificationSpecArray = await getNotificationSpecsByScheduleOncePerDay(req.body.scheduleAt);
    if (notificationSpecArray.length === 0) {
      console.log("Function sendScheduledNotifications() has run earlier today. Exiting...");
      return res.status(208).json(getResponseJSON("Function has run earlier today.", 208));
    }

    await setupSendGrid();
    await setupTwilio();
    await Promise.all(notificationSpecArray.map(handleNotificationSpec));
    console.log("Finished sending out notifications.");
    return res.status(200).json(getResponseJSON("Finished sending out notifications", 200));
  } catch (error) {
    console.error("Error occurred running function sendScheduledNotifications.", error);
    return res.status(500).json(getResponseJSON("Internal Server Error!", 500));
  } finally {
    isSendingNotifications = false;
  }
}

function getTimeParams(notificationSpec) {
  const { primaryField, time } = notificationSpec;

  if (/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z/.test(primaryField)) {
    const startTime = getAdjustedTime(primaryField, time.start.day, time.start.hour, time.start.minute);
    const stopTime = getAdjustedTime(primaryField, time.stop.day, time.stop.hour, time.stop.minute);
    const currentTime = new Date();
    if (startTime > currentTime || currentTime > stopTime) return null;
    return { startTimeStr: "", stopTimeStr: "", timeField: "" };
  }

  const startTime = getAdjustedTime(new Date(), -time.start.day, -time.start.hour, -time.start.minute);
  const stopTime = getAdjustedTime(new Date(), -time.stop.day, -time.stop.hour, -time.stop.minute);
  return {
    startTimeStr: startTime.toISOString(),
    stopTimeStr: stopTime.toISOString(),
    timeField: primaryField,
  };
}

async function handleNotificationSpec(notificationSpec) {
  const timeParams = getTimeParams(notificationSpec);
  if (!timeParams) return;

  const readableSpecString = notificationSpec.category + ", " + notificationSpec.attempt;
  const emailField = notificationSpec.emailField ?? "";
  const phoneField = notificationSpec.phoneField ?? "";
  const firstNameField = notificationSpec.firstNameField ?? "";
  const preferredNameField = notificationSpec.preferredNameField ?? "";
  const newsletterCategories = ["newsletter", "eNewsletter", "anniversaryNewsletter"];
  
  let fieldsToFetch = ["Connect_ID", "token", "state.uid", conceptIds.preferredLanguage.toString()];
  firstNameField && fieldsToFetch.push(firstNameField);
  preferredNameField && fieldsToFetch.push(preferredNameField);
  emailField && fieldsToFetch.push(emailField);
  phoneField && fieldsToFetch.push(phoneField);
  notificationSpec.notificationType.includes("sms") && fieldsToFetch.push(conceptIds.canWeText.toString());

  let emailInSpec = notificationSpec.email || {};
  let smsInSpec = notificationSpec.sms || {};
  let emailHasToken = false;
  let emailHasLoginDetails = false;
  let emailCount = { total: 0 };
  let smsCount = { total: 0 };

  for (const lang of langArray) {
    if (emailInSpec[lang]?.body) {
      let emailBody = emailInSpec[lang].body;
      if (!newsletterCategories.includes(notificationSpec.category)) {
        emailBody = converter.makeHtml(emailBody);
      }

      emailBody = emailBody.replace(/<firstName>/g, "{{firstName}}");
      if (emailBody.includes("${token}")) {
        emailHasToken = true;
        emailBody = emailBody.replace(/\${token}/g, "{{token}}");
      }

      if (emailBody.includes("<loginDetails>")) {
        emailHasLoginDetails = true;
        emailBody = emailBody.replace(/<loginDetails>/g, "{{loginDetails}}");
        fieldsToFetch.push(
          `${conceptIds.signInMechanism}`,
          `${conceptIds.authenticationPhone}`,
          `${conceptIds.authenticationEmail}`
        );
      }

      emailInSpec[lang].body = emailBody;
    }

    emailCount[lang] = 0;
    smsCount[lang] = 0;
  }

  const limit = 1000; // SendGrid has a batch limit of 1000
  let previousToken = "";
  let hasNext = true;
  let fetchedDataArray = [];
  let conditions = [];
  if (notificationSpec.conditions) {
    conditions = JSON.parse(notificationSpec.conditions);
  }

  while (hasNext) {
    try {
      fetchedDataArray = await getParticipantsForNotificationsBQ({
        notificationSpecId: notificationSpec.id,
        startTimeStr: timeParams.startTimeStr,
        stopTimeStr: timeParams.stopTimeStr,
        timeField: timeParams.timeField,
        conditions,
        fieldsToFetch,
        limit,
        previousToken,
      });
    } catch (error) {
      console.error(`getParticipantsForNotificationsBQ() error running spec ID ${notificationSpec.id}.`, error);
      break;
    }

    if (fetchedDataArray.length === 0) break;

    hasNext = fetchedDataArray.length === limit;
    if (hasNext) {
      previousToken = fetchedDataArray[fetchedDataArray.length - 1].token;
    }

    let notificationData = {};
    for (const lang of langArray) {
      notificationData[lang] = {
        emailRecordArray: [],
        emailPersonalizationArray: [],
        smsRecordArray: [],
      };
    }

    for (const fetchedData of fetchedDataArray) {
      if (!fetchedData[emailField] && !fetchedData[phoneField]) continue;

      const uniqId = uuid();
      const emailId = uniqId + "-1";
      const smsId = uniqId + "-2";
      const currDateTime = new Date().toISOString();
      const firstName = fetchedData[preferredNameField] || fetchedData[firstNameField];
      const prefLang = cidToLangMapper[fetchedData[conceptIds.preferredLanguage]] || "english";
      const recordCommonData = {
        notificationSpecificationsID: notificationSpec.id,
        attempt: notificationSpec.attempt,
        category: notificationSpec.category,
        token: fetchedData.token,
        uid: fetchedData.state.uid,
        read: false,
      };

      if (emailInSpec[prefLang]?.body && validEmailFormat.test(fetchedData[emailField])) {
        let substitutions = { firstName };
        let currEmailBody = emailInSpec[prefLang].body.replace(/{{firstName}}/g, firstName);

        if (emailHasLoginDetails) {
          let loginDetails = "";
          if (fetchedData[conceptIds.signInMechanism] === "phone" && fetchedData[conceptIds.authenticationPhone]) {
            loginDetails = redactPhoneLoginInfo(fetchedData[conceptIds.authenticationPhone]);
          } else if (
            fetchedData[conceptIds.signInMechanism] === "password" &&
            fetchedData[conceptIds.authenticationEmail]
          ) {
            loginDetails = redactEmailLoginInfo(fetchedData[conceptIds.authenticationEmail]);
          } else if (
            fetchedData[conceptIds.signInMechanism] === "passwordAndPhone" &&
            fetchedData[conceptIds.authenticationEmail] &&
            fetchedData[conceptIds.authenticationPhone]
          ) {
            loginDetails =
              redactPhoneLoginInfo(fetchedData[conceptIds.authenticationPhone]) +
              ", " +
              redactEmailLoginInfo(fetchedData[conceptIds.authenticationEmail]);
          } else {
            console.log("No login details found for participant with token:", fetchedData.token);
            continue;
          }

          substitutions.loginDetails = loginDetails;
          currEmailBody = currEmailBody.replace(/{{loginDetails}}/g, loginDetails);
        }

        if (emailHasToken) {
          substitutions.token = fetchedData.token;
          currEmailBody = currEmailBody.replace(/{{token}}/g, fetchedData.token);
        }

        notificationData[prefLang].emailPersonalizationArray.push({
          to: fetchedData[emailField],
          substitutions,
          custom_args: {
            connect_id: fetchedData.Connect_ID,
            token: fetchedData.token,
            notification_id: emailId,
            gcloud_project: process.env.GCLOUD_PROJECT,
          },
        });

        notificationData[prefLang].emailRecordArray.push({
          ...recordCommonData,
          id: emailId,
          notificationType: "email",
          language: prefLang,
          email: fetchedData[emailField],
          notification: {
            title: emailInSpec[prefLang].subject,
            body: currEmailBody,
            time: currDateTime,
          },
        });
      }

      // Handle mixed data types of conceptIds.canWeText. Remove this after fixing bug causing string data type.
      let canWeText = fetchedData[conceptIds.canWeText];
      if (typeof canWeText === "object" && canWeText.integer) {
        canWeText = canWeText.integer;
      }

      if (smsInSpec[prefLang]?.body && fetchedData[phoneField]?.length >= 10 && canWeText === conceptIds.yes) {
        const phoneNumber = fetchedData[phoneField].replace(/\D/g, "");
        if (phoneNumber.length >= 10) {
          const smsTo = `+1${phoneNumber.slice(-10)}`;
          notificationData[prefLang].smsRecordArray.push({
            ...recordCommonData,
            id: smsId,
            notificationType: "sms",
            language: prefLang,
            phone: smsTo,
            notification: {
              body: smsInSpec[prefLang].body,
              time: currDateTime,
            },
          });
        }
      }
    }
    
    for (const lang of langArray) {
      let { emailRecordArray, emailPersonalizationArray, smsRecordArray } = notificationData[lang];
      if (emailPersonalizationArray.length > 0) {
        const emailBatch = {
          from: {
            name: process.env.SG_FROM_NAME || "Connect for Cancer Prevention Study",
            email: process.env.SG_FROM_EMAIL || "no-reply-myconnect@mail.nih.gov",
          },
          subject: emailInSpec[lang].subject,
          html: emailInSpec[lang].body,
          personalizations: emailPersonalizationArray,
          tracking_settings: {
            subscription_tracking: {
              enable: true,
              html: unsubscribeTextObj[lang] || unsubscribeTextObj.english,
            },
          },
        };
  
        try {
          await sgMail.send(emailBatch);
          await saveNotificationBatch(emailRecordArray);
          emailCount[lang] += emailRecordArray.length;
        } catch (error) {
          if (error.message.startsWith("saveNotificationBatch")) {
            console.error(`Error saving email records for ${notificationSpec.id}(${readableSpecString}).`, error);
          } else {
            console.error(`Error sending emails for ${notificationSpec.id}(${readableSpecString}).`, error);
          }
  
          break;
        }
      }
  
      if (smsRecordArray.length > 0) {
        smsBatchSender.addToQueue(smsRecordArray);
      }
    }

  }

  smsBatchSender.markSpecEnd(notificationSpec.id);
  const { sentCounts, failedCounts } = await smsBatchSender.waitForSpec(notificationSpec.id);

  let totalFailed = 0;
  for (const lang of langArray) {
    smsCount[lang] = sentCounts[lang] || 0;
    emailCount.total += emailCount[lang];
    smsCount.total += smsCount[lang];
    totalFailed += failedCounts[lang] || 0;
  }

  let messageArray = [`Finished notification spec: ${notificationSpec.id}(${readableSpecString})`];
  if (emailCount.total === 0) {
    messageArray.push("No emails sent");
  } else {
    for (const lang of langArray) {
      messageArray.push(`Email (${lang}) sent: ${emailCount[lang]}`);
    }
  }

  if (smsCount.total === 0) {
    messageArray.push("No SMS sent");
  } else {
    for (const lang of langArray) {
      messageArray.push(`SMS (${lang}) sent: ${smsCount[lang]}`);
    }
    if (totalFailed > 0) {
      messageArray.push(`SMS failed: ${totalFailed}`);
    }
  }

  console.log(messageArray.join(". ") + ".");
}

const storeNotificationSchema = async (req, res, authObj) => {
  logIPAddress(req);
  setHeaders(res);

  if (req.method === "OPTIONS") return res.status(200).json({ code: 200 });

  if (req.method !== "POST") return res.status(405).json(getResponseJSON("Only POST requests are accepted!", 405));

  if (!authObj) return res.status(401).json(getResponseJSON("Authorization failed!", 401));

  if (req.body.data === undefined || Object.keys(req.body.data).length < 1)
    return res.status(400).json(getResponseJSON("Bad requuest.", 400));

  try {
    const schema = req.body.data;
    if (schema.id) {
      const { retrieveNotificationSchemaByID } = require("./firestore");
      const docID = await retrieveNotificationSchemaByID(schema.id);
      if (docID === "") return res.status(404).json(getResponseJSON("Invalid notification Id.", 404));

      const { updateNotificationSchema } = require("./firestore");
      schema["modifiedAt"] = new Date().toISOString();
      if (authObj.userEmail) schema["modifiedBy"] = authObj.userEmail;
      await updateNotificationSchema(docID, schema);
    } else {
      schema["id"] = uuid();
      const { storeNewNotificationSchema } = require("./firestore");
      schema["createdAt"] = new Date().toISOString();
      if (authObj.userEmail) schema["createdBy"] = authObj.userEmail;
      await storeNewNotificationSchema(schema);
    }

    return res.status(200).json({ message: "Success!", code: 200, data: [{ schemaId: schema.id }] });
  } catch (error) {
    console.error("Error occurred storing notification schema.", error);
    return res.status(500).json({ message: error.message, code: 500, data: [] });
  }

};

const retrieveNotificationSchema = async (req, res, authObj) => {
  logIPAddress(req);
  setHeaders(res);

  if (req.method === "OPTIONS") return res.status(200).json({ code: 200 });

  if (req.method !== "GET") return res.status(405).json(getResponseJSON("Only GET requests are accepted!", 405));

  if (!authObj) return res.status(401).json(getResponseJSON("Authorization failed!", 401));

  if (!req.query.category)
    return res.status(400).json(getResponseJSON("category is missing in request parameter!", 400));

  const category = req.query.category;
  const getDrafts = req.query.drafts === "true";
  const { retrieveNotificationSchemaByCategory } = require("./firestore");

  try {
    const schemaArray = await retrieveNotificationSchemaByCategory(category, getDrafts);
    if (schemaArray.length === 0)
      return res.status(404).json({ data: [], message: `Notification schema not found for given category - ${category}`, code: 404 });

    return res.status(200).json({ data: schemaArray, code: 200 });
  } catch (error) {
    console.error("Error retrieving notification schemas.", error);
    return res.status(500).json({ data: [], message: error.message, code: 500 });
  }
};

const getParticipantNotification = async (req, res, authObj) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method === 'OPTIONS') return res.status(200).json({code: 200});
        
    if (req.method !== 'GET') return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    
    let obj = {};
    if (authObj) obj = authObj;
    else {
        const { APIAuthorization } = require('./shared');
        const authorized = await APIAuthorization(req);
        if(authorized instanceof Error){
            return res.status(500).json(getResponseJSON(authorized.message, 500));
        }
    
        if(!authorized){
            return res.status(401).json(getResponseJSON('Authorization failed!', 401));
        }
    
        const { isParentEntity } = require('./shared');
        obj = await isParentEntity(authorized);
    }

    if(!req.query.token) return res.status(400).json(getResponseJSON('token is missing in request parameter!', 400));
    const token = req.query.token;
    const isParent = obj.isParent;
    const siteCodes = obj.siteCodes;
    const { getNotificationHistoryByParticipant } = require('./firestore');
    const data = await getNotificationHistoryByParticipant(token, siteCodes, isParent);
    if(!data) return res.status(400).json(getResponseJSON('Invalid token or you are not authorized to access data for given token', 200));

    return res.status(200).json({data, code: 200})
}

const getSiteNotification = async (req, res, authObj) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method === 'OPTIONS') return res.status(200).json({code: 200});
        
    if (req.method !== 'GET') return res.status(405).json(getResponseJSON('Only GET requests are accepted!', 405));
    
    let obj = {};
    if (authObj) obj = authObj;
    else {
        const { APIAuthorization } = require('./shared');
        const authorized = await APIAuthorization(req);
        if(authorized instanceof Error){
            return res.status(500).json(getResponseJSON(authorized.message, 500));
        }
    
        if(!authorized){
            return res.status(401).json(getResponseJSON('Authorization failed!', 401));
        }
    
        const { isParentEntity } = require('./shared');
        obj = await isParentEntity(authorized);
    }

    const isParent = obj.isParent;
    const siteId = obj.id;
    const { retrieveSiteNotifications } = require('./firestore');
    const data = await retrieveSiteNotifications(siteId, isParent);
    if(data !== false){
        markAllNotificationsAsAlreadyRead(data.map(dt => dt.id), 'siteNotifications');
    }
    return res.status(200).json({data, code: 200})
}

const sendEmailLink = async (req, res) => {
    if (req.method !== "POST") {
        return res
            .status(405)
            .json(getResponseJSON("Only POST requests are accepted!", 405));
    }
    try {
        const { email, continueUrl, preferredLanguage } = req.body;
        const [clientId, clientSecret, tenantId, magicLink] = await Promise.all(
            [
                getSecret(process.env.APP_REGISTRATION_CLIENT_ID),
                getSecret(process.env.APP_REGISTRATION_CLIENT_SECRET),
                getSecret(process.env.APP_REGISTRATION_TENANT_ID),
                generateSignInWithEmailLink(email, continueUrl),
            ]
        );

        const cleanMagicLink = cleanContinueUrl(magicLink);

        const params = new URLSearchParams();
        params.append("grant_type", "client_credentials");
        params.append("scope", "https://graph.microsoft.com/.default");
        params.append("client_id", clientId);
        params.append("client_secret", clientSecret);

        const resAuthorize = await fetch(
            `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
            {
                method: "POST",
                headers: {
                    "Content-Type":
                        "application/x-www-form-urlencoded;charset=UTF-8",
                },
                body: params,
            }
        );

        const { access_token } = await resAuthorize.json();
        const body = {
            message: {
                subject:
                    preferredLanguage === conceptIds.spanish
                        ? "Inicie sesión para Estudio Connect para la Prevención del Cáncer"
                        : "Sign in to Connect for Cancer Prevention Study",
                body: {
                    contentType: "html",
                    content: getTemplateForEmailLink(
                        email,
                        cleanMagicLink,
                        preferredLanguage
                    ),
                },
                toRecipients: [
                    {
                        emailAddress: {
                            address: email,
                        },
                    },
                ],
            },
        };
        const response = await fetch(
            `https://graph.microsoft.com/v1.0/users/${nihMailbox}/sendMail`,
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Authorization: `Bearer ${access_token}`,
                },
                body: JSON.stringify(body),
            }
        );
        const { status, statusText: code } = response;
        return res.status(202).json({ status, code });
        
    } catch (err) {
        console.error(`Error in sendEmailLink(). ${err.message}`);
        return res
            .status(500)
            .json({
                data: [],
                message: `Error in sendEmailLink(). ${err.message}`,
                code: 500,
            });
    }
};

/**
 * Properly cleans the continueUrl parameter by finding where it should end
 * @param {string} url - The full authentication URL
 * @returns {string} - URL with cleaned continueUrl parameter
 */
const cleanContinueUrl = (url) => {
    const normalizedUrl = url.replace(/&amp;/g, '&');
    const continueUrlIndex = normalizedUrl.indexOf('continueUrl=');
    
    if (continueUrlIndex === -1) {
        return url;
    }

    const beforeContinueUrl = normalizedUrl.substring(0, continueUrlIndex + 'continueUrl='.length);
    const afterContinueUrlStart = normalizedUrl.substring(continueUrlIndex + 'continueUrl='.length);
    const baseUrlMatch = afterContinueUrlStart.match(/^(https:\/\/[^&#]+)/);
    
    if (!baseUrlMatch) {
        return url;
    }
    
    const baseUrl = baseUrlMatch[1];

    return beforeContinueUrl + baseUrl;
}

const dryRunNotificationSchema = async (req, res) => {
  if (req.method !== "GET") {
    return res.status(405).json({ data: [], message: "Only GET requests are accepted!", code: 405 });
  }

  if (!req.query.schemaId) {
    return res.status(400).json({ data: [], message: "schemaId is missing in request parameter!", code: 400 });
  }

  let spec = null;
  try {
    spec = await getNotificationSpecById(req.query.schemaId);
    if (!spec) {
      const message = `Notification spec ID ${req.query.schemaId} isn't found.`;
      return res.status(404).json({ data: [], message, code: 404 });
    }
    const { data, message, code } = await handleDryRun(spec);
    return res.status(code).json({ data, message, code });

  } catch (error) {
    return res.status(500).json({ data: [], message: JSON.stringify(error, null, 2), code: 500 });
  }

};

async function handleDryRun(spec) {
  const timeParams = getTimeParams(spec);
  if (!timeParams) return { data: [], message: "Ok", code: 200 };

  const emailInSpec = spec.email || {};
  const smsInSpec = spec.sms || {};
  const emailField = spec.emailField ?? "";
  const phoneField = spec.phoneField ?? "";
  let fieldsToFetch = ["token", "Connect_ID", conceptIds.preferredLanguage.toString()];
  emailField && fieldsToFetch.push(emailField);
  phoneField && fieldsToFetch.push(phoneField);
  spec.notificationType.includes("sms") && fieldsToFetch.push(conceptIds.canWeText.toString());

  const limit = 1000;
  let previousToken = "";
  let hasNext = true;
  let fetchedDataArray = [];
  let countObj = { email: {}, sms: {} };
  let conditions = [];
  if (spec.conditions) {
    conditions = JSON.parse(spec.conditions);
  }

  for (const lang of langArray) {
    countObj.email[lang] = 0;
    countObj.sms[lang] = 0;
  }

  while (hasNext) {
    try {
      fetchedDataArray = await getParticipantsForNotificationsBQ({
        notificationSpecId: spec.id,
        startTimeStr: timeParams.startTimeStr,
        stopTimeStr: timeParams.stopTimeStr,
        timeField: timeParams.timeField,
        conditions,
        fieldsToFetch,
        limit,
        previousToken,
      });
    } catch (error) {
      console.error(`Error dry running spec ID ${spec.id}.`, error);
      return { data: [countObj], message: JSON.stringify(error, null, 2), code: 500 };
    }

    if (fetchedDataArray.length === 0) break;
    hasNext = fetchedDataArray.length === limit;
    if (hasNext) {
      previousToken = fetchedDataArray[fetchedDataArray.length - 1].token;
    }

    for (const fetchedData of fetchedDataArray) {
      if (!fetchedData[emailField] && !fetchedData[phoneField]) continue;

      const prefLang = cidToLangMapper[fetchedData[conceptIds.preferredLanguage]] || "english";

      if (emailInSpec[prefLang] && validEmailFormat.test(fetchedData[emailField])) {
        countObj.email[prefLang]++;
      }

      if (
        smsInSpec[prefLang] &&
        fetchedData[phoneField]?.length >= 10 &&
        fetchedData[conceptIds.canWeText] === conceptIds.yes
      ) {
        countObj.sms[prefLang]++;
      }
    }
  }

  return { data: [countObj], message: "Ok", code: 200 };
}

const sendInstantNotification = async (requestData) => {
  const notificationSpec = await getNotificationSpecByCategoryAndAttempt(requestData.category, requestData.attempt);
  const errMsg = `Error sending instant notification (${requestData.category}, ${requestData.attempt}) to participant with ID ${requestData.connectId}`;

  if (!notificationSpec) {
    throw new Error(`${errMsg}. Notification spec not found.`);
  }

  const isNotificationSent = await checkIsNotificationSent(requestData.token, notificationSpec.id);
  if (isNotificationSent) {
    throw new Error(`${errMsg}. Notification already sent.`);
  }

  const uuidStr = uuid();
  const emailOfPrefLang = notificationSpec.email[requestData.preferredLanguage] || notificationSpec.email.english;
  const currEmailBody = emailOfPrefLang.body
    .replace(/{{firstName}}/g, requestData.substitutions.firstName)
    .replace(/{{loginDetails}}/g, requestData.substitutions.loginDetails);

  const emailDataToSg = {
    from: {
      name: process.env.SG_FROM_NAME || "Connect for Cancer Prevention Study",
      email: process.env.SG_FROM_EMAIL || "no-reply-myconnect@mail.nih.gov",
    },
    subject: emailOfPrefLang.subject,
    html: emailOfPrefLang.body,
    personalizations: [
      {
        to: requestData.email,
        substitutions: requestData.substitutions,
        custom_args: {
          connect_id: requestData.connectId,
          token: requestData.token,
          notification_id: uuidStr,
          gcloud_project: process.env.GCLOUD_PROJECT,
        },
      },
    ],
    tracking_settings: {
      subscription_tracking: {
          enable: true,
          html: unsubscribeTextObj[requestData.preferredLanguage] || unsubscribeTextObj.english
      },
    },
  };

  const currEmailRecord = {
    id: uuidStr,
    notificationType: "email",
    language: requestData.preferredLanguage,
    email: requestData.email,
    notification: {
      title: emailOfPrefLang.subject,
      body: currEmailBody,
      time: new Date().toISOString(),
    },
    notificationSpecificationsID: notificationSpec.id,
    attempt: requestData.attempt,
    category: requestData.category,
    token: requestData.token,
    uid: requestData.uid,
    read: false,
  };

  try {
    await setupSendGrid();
    await sgMail.send(emailDataToSg);
    await storeNotification(currEmailRecord);
  } catch (err) {
    console.error(`Error with data emailDataToSg: ${emailDataToSg}`); // Can be Removed after troubleshooting
    throw new Error(errMsg, { cause: err });
  }
};

const handleIncomingSms = async (req, res) => {
  if (!isTwilioSetup) {
    await setupTwilio();
  }

  if (!req.body || req.body.MessagingServiceSid !== messagingServiceSid) {
    return res.status(400).json(getResponseJSON("Bad request!", 400));
  }

  const { OptOutType: optinOptoutType } = req.body;
  if (["START", "STOP"].includes(optinOptoutType)) {
    const isSmsPermitted = optinOptoutType === "START";
    try {
      await updateSmsPermission(req.body.From, isSmsPermitted);
    } catch (error) {
      console.error("Error updating sms permission to 'participants' collection.", error);
    }
  }
  
  return res.sendStatus(204);
};

module.exports = {
  subscribeToNotification,
  retrieveNotifications,
  sendScheduledNotifications,
  storeNotificationSchema,
  retrieveNotificationSchema,
  getParticipantNotification,
  sendEmail,
  getSiteNotification,
  sendEmailLink,
  dryRunNotificationSchema,
  sendInstantNotification,
  handleDryRun,
  handleIncomingSms,
  SmsBatchSender,
};

const { processRequestAKitConditions } = require('./firestore');

const requestAKitEligibilityUpdates = async () => {
    try {
        await processRequestAKitConditions();
    } catch(err) {
        console.error('Error retrieving requestAKitConditions', err);
    }
};

module.exports = {
    requestAKitEligibilityUpdates
};
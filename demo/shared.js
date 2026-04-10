const { getResponseJSON, developmentTier } = require('../utils/shared');

const validRounds = ['A1', 'A2', 'A3', 'A4'];
const validCollectionTypes = ['Blood', 'Urine', 'Mouthwash'];
const validStatuses = ['Complete', 'Refused'];

const isDev = (req, res) => {
    if (developmentTier !== 'DEV') {
        res.status(403).json(getResponseJSON('API not available in this environment.', 403));
        return false;
    }
    return true;
};

module.exports = {
    validRounds,
    validCollectionTypes,
    validStatuses,
    isDev,
};

const {
    removeParticipantsDataDestruction,
    removeUninvitedParticipants,
} = require(`./firestore`);
const { getResponseJSON } = require(`./shared`);

const getFailureMessage = (reason) => (
    reason instanceof Error
        ? reason.message
        : String(reason)
);

const runParticipantDataCleanup = async () => {
    console.log(`Start cleaning up participant data`);
    const [dataDestructionResult, uninvitedResult] = await Promise.allSettled([
        removeParticipantsDataDestruction(),
        removeUninvitedParticipants(),
    ]);

    const failures = [];

    if (dataDestructionResult.status === "rejected") {
        console.error("Error in removeParticipantsDataDestruction:", dataDestructionResult.reason);
        failures.push(`removeParticipantsDataDestruction: ${getFailureMessage(dataDestructionResult.reason)}`);
    }
    if (uninvitedResult.status === "rejected") {
        console.error("Error in removeUninvitedParticipants:", uninvitedResult.reason);
        failures.push(`removeUninvitedParticipants: ${getFailureMessage(uninvitedResult.reason)}`);
    }

    console.log(`Complete cleanup of participant data`);

    const results = {
        dataDestructionStatus: dataDestructionResult.status,
        uninvitedStatus: uninvitedResult.status,
    };

    if (failures.length > 0) {
        const error = new Error(`Participant data cleanup failed: ${failures.join("; ")}`);
        error.results = results;
        throw error;
    }

    return results;
};

const participantDataCleanup = async (req, res) => {
    // Preserve direct invocation used by existing unit tests.
    console.log("Received request for participantDataCleanup");
    if (!req || !res) {
        return runParticipantDataCleanup();
    }

    if (req.method !== "POST") {
        return res.status(405).json(getResponseJSON("Method not allowed. Use POST.", 405));
    }

    try {
        const results = await runParticipantDataCleanup();

        return res.status(200).json({
            ...getResponseJSON("Participant data cleanup completed.", 200),
            results,
        });
    } catch (error) {
        console.error("Error in participantDataCleanup:", error);
        return res.status(500).json({
            ...getResponseJSON("Failed to clean participant data.", 500),
            results: error.results,
        });
    }
};

module.exports = {
    runParticipantDataCleanup,
    participantDataCleanup,
};

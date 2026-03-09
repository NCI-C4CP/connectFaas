const {
    removeParticipantsDataDestruction,
    removeUninvitedParticipants,
} = require(`./firestore`);

const participantDataCleanup = async () => {
    console.log(`Start cleaning up participant data`);
    const [dataDestructionResult, uninvitedResult] = await Promise.allSettled([
        removeParticipantsDataDestruction(),
        removeUninvitedParticipants(),
    ]);

    if (dataDestructionResult.status === "rejected") {
        console.error("Error in removeParticipantsDataDestruction:", dataDestructionResult.reason);
    }
    if (uninvitedResult.status === "rejected") {
        console.error("Error in removeUninvitedParticipants:", uninvitedResult.reason);
    }

    console.log(`Complete cleanup of participant data`);
};

module.exports = {
    participantDataCleanup,
};

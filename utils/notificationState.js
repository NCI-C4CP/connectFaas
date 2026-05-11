const PROVIDER_SEND_STARTED_STATES = Object.freeze([
  "provider_send_in_flight",
  "provider_acceptance_unknown",
]);

const PROVIDER_SEND_STARTED_STATE_SET = new Set(PROVIDER_SEND_STARTED_STATES);

const isProviderSendStartedState = (processingState = "") =>
  PROVIDER_SEND_STARTED_STATE_SET.has(processingState);

module.exports = {
  PROVIDER_SEND_STARTED_STATES,
  isProviderSendStartedState,
};

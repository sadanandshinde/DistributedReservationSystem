package TwoPhaseCommit;

public enum TransactionState {
    INITIAL,
    SEND_VOTEREQ,
    WAITING_VOTES,
    SEND_COMMIT,
    SEND_ABORT,
    WAITING_VOTEREQ,
    VOTE_NO,
    VOTE_YES,
    WAITING_DECISION,
    COMMITTED,
    ABORTED
}
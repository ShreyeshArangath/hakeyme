package api

var (
	InvalidKey               = errors.New("Found an invalid key")
	InvalidTTL               = errors.New("Invalid TTL")
	ExpiredKey               = errors.New("Specified key has expired")
	TransactionClosed        = errors.New("Transaction was closed")
	DatabaseConnectionClosed = errors.New("Database connection was closed")
	TransactionReadOnly      = errors.New("Transaction only supports read")
)



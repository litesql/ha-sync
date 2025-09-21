package config

const (
	// Common config
	ClientName  = "client_name"   // Client name used to connect to the broker
	Servers     = "servers"       // Comma-separated list of brokers servers
	Timeout     = "timeout"       // timeout in milliseconds
	Username    = "username"      // username to connect to broker
	Password    = "password"      // password to connect to broker
	Token       = "token"         // user token
	CertFile    = "cert_file"     // TLS: path to certificate file
	CertKeyFile = "cert_key_file" // TLS: path to .pem certificate key file
	CertCAFile  = "ca_file"       // TLS: path to CA certificate file
	Insecure    = "insecure"      // TLS: Insecure skip TLS verification
	Logger      = "logger"        // Log errors to "stdout, stderr or file:/path/to/log.txt"

	DefaultReplicationVTabName = "ha"
)

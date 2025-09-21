package extension

import (
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/ha-sync/config"
)

func registerFunc(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
	if err := api.CreateModule(config.DefaultReplicationVTabName, &JetStreamSubscriberModule{}, sqlite.ReadOnly(false)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateFunction("ha_info", &Info{}); err != nil {
		return sqlite.SQLITE_ERROR, err
	}

	return sqlite.SQLITE_OK, nil
}

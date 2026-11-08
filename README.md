# blvm-upstream

Outbound connectors for mempool.space, Esplora, and bitcoind JSON-RPC, behind the `blvm.upstream.v1` contract.

HTTP goes through the node broker. Tests use a local mock and do not call mempool.space or blockstream.

It is not a default module pin. The operator allow-list is `[module_network.blvm-upstream] http_allow`.

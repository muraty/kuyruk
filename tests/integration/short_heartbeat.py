from kuyruk import Kuyruk, Config

config = Config()
config.from_pyfile('/tmp/kuyruk_config.py')

# Detect a dead connection in seconds instead of minutes.
config.RABBIT_HEARTBEAT = 2
config.RABBIT_READ_TIMEOUT = 10
config.RABBIT_WRITE_TIMEOUT = 2
config.TCP_USER_TIMEOUT = 5
config.WORKER_RECONNECT_INTERVAL = 1

kuyruk = Kuyruk(config=config)

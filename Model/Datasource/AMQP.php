<?php

class AMQP extends DataSource {

/**
 * The default configuration
 *
 * @var array
 */
	protected $_baseConfig = array(
		'host' => 'localhost',
		'port' => 5672,
		'user' => 'guest',
		'password' => 'guest',
		'vhost' => '/',
		'exchange' => 'default',
		'exchangeType' => AMQP_EX_TYPE_DIRECT,
		'exchangeFlags' => 0,
		'types' => array('*')
	);

/**
 * AMQP Connection
 *
 * @var AMQPConnection
 */
	protected $_connection;

/**
 * AMQP Exchange
 *
 * @var AMQPExchange
 */
	protected $_exchange;

/**
 * Constructor
 *
 * @param array $config
 */
    function __construct($config = array()) {
        parent::__construct();
        $this->setConfig(array_merge($this->_baseConfig, $config));
        $this->fullDebug = Configure::read('debug') > 1;
        $this->createConnection();
        $this->connect();
    }

/**
 * Creates a connection to the AMQP Server
 *
 * @return void
 */
	public function createConnection() {
		$credentials = array(
			'host' => $this->config['host'],
			'port' => $this->config['port'],
			'vhost' => $this->config['vhost'],
			'login' => $this->config['user'],
			'password' => $this->config['password'],
		);

        $this->_connection = new AMQPConnection($credentials);
	}

/**
 * Connects to the RabbitMQ Server
 *
 * @return bool
 */
	public function connect() {
        if ($this->isConnected()) {
            return true;
        }
        
        try {
		    if (!$this->_connection->connect()) {
                return false;
            }

            $this->_exchange = new AMQPExchange($this->_connection);
		    $this->_exchange->declare(
                $this->config['exchange'],
                $this->config['exchangeType'],
                $this->config['exchangeFlags']
            );
        } catch (Exception $e) {
            throw new CakeException("RabbitMQ Datasource - " . $e->getMessage());
        }

        return true;
	}
    
/**
 * Disconnects the connection
 *
 * @return bool
 */
    function disconnect() {
        if ($this->isConnected()) {
            return $this->_connection->disconnect();
        }

        return true;
    }

/**
 * Returns the connection status
 *
 * @return bool
 */
    public function isConnected() {
        if (!$this->_connection) {
            return false;
        }
        
        return $this->_connection->isConnected();
    }

/**
 * @param string  $type
 * @param mixed $message
 * @return bool
 */
	public function publish($type, $message, $eta = null) {
        $this->connect();
        
		if (!in_array($type, $this->config['types']) &&
            $this->config['types'] !== array('*')) {
			return false;
		}

        if (is_array($message)) {
            $message = json_encode($message);
        }

		$result = $this->_exchange->publish($message, $type);
        $this->disconnect();

        return $result;
	}

/**
 * All calls to methods on the model are routed through this method
 *
 * @param mixed $method
 * @param mixed $params
 * @param mixed $Model
 * @access public
 * @return boolean
 */
    public function query($method, $params, &$Model) {
        $startQuery = microtime(true);
        switch ($method) {
            case 'publish':
                $result = $this->dispatchMethod($method, $params);
                return $result;
            default:
                trigger_error("RabbitMQ::query - Unkown method {$method}.", E_USER_WARNING);
                return false;
        }
    }

/**
 * Returns available sources
 *
 * @see Mode::useTable
 * @return array
 */
    function listSources($data = null) {
        return array('tasks');
    }
}
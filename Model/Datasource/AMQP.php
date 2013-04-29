<?php
App::uses('DataSource', 'Model/Datasource');
/**
 *
 */
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
	 * AMQP Channel
	 *
	 * @var AMQPChannel
	 */
	protected $_channel;

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
	 * Connects to the AMQP Server
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

			$this->_channel = new AMQPChannel($this->_connection);
			$this->_exchange = new AMQPExchange($this->_channel);
			$this->_exchange->setName($this->config['exchange']);
			$this->_exchange->setType($this->config['exchangeType']);
			$this->_exchange->setFlags($this->config['exchangeFlags']);
			$this->_exchange->declare();
		} catch (Exception $e) {
			throw new CakeException("AMQP Datasource - " . $e->getMessage());
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
	 * @param string $type
	 * @param string|array $message
	 * @return bool
	 */
	public function publish($message, $routingKey, $flags = AMQP_NOPARAM, $attributes = array()) {
		$this->connect();

		if (!in_array($routingKey, $this->config['types']) && $this->config['types'] !== array('*')) {
			return false;
		}

		if (is_array($message)) {
			$message = json_encode($message);
		}

		$result = null;
		try {
			$result = $this->_exchange->publish($message, $routingKey, $flags, $attributes);
		} catch (AMQPExchangeException $e) {

		} catch (AMQPChannelException $e) {

		} catch (AMQPConnectionException $e) {

		}

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
		switch ($method) {
			case 'publish':
				$result = $this->dispatchMethod($method, $params);
				return $result;
			default:
				trigger_error("RabbitMQ::query - Unknown method {$method}.", E_USER_WARNING);
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

<?php

    define('DB_PATH', './db/');

    class base {
        protected $error_list = null;
        
        public function gen_uid() {
            $microtime = explode(' ', microtime());
            return substr(hash('md4', $microtime[1]), 0, 8).'-'.substr(hash('md4', file_get_contents('/proc/stat').mt_rand()), 8, 8).'-'.substr(hash('md4', $microtime[0]), 16, 8).'-'.substr(hash('md4', file_get_contents('/proc/net/arp')), 24, 8);
        }
        
        protected function setError($num=null) {
            if($this->error_list == null) {
                $this->error_list = $this->get_error_list();
            }
            
            if(!isset($this->error_list[$num]) || $num==null) {
                return array("Unknown Error", get_class($this));
            }
            
            return array($this->error_list[$num], get_class($this), $num);
        }
        
        public function is_callback(
    }
    
    function is_assoc($array) {
      return (bool)count(array_filter(array_keys($array), 'is_string'));
    }
    
    function is_closure($cb) {
        return is_object($cb) && ($cb instanceof Closure);
    }

    class meta extends base {
        function __construct() {
            $this->conn = new SQLite3('./db/meta.db');
            
        }

        function getTableByName($name, $db_uid) {
            return $this->conn->querySingle("SELECT * FROM tables where name='".$name."' LIMIT 1;", True);
        }

        function getTableByUid($uid) {
            return $this->conn->querySingle("SELECT * FROM tables where uid='".$uid."' LIMIT 1;", True);
        }

        function tableExists($uid) {
            return (!empty($this->getTableByUid($uid))) ? True : False;
        }

        function getDbByName($name) {
            return $this->conn->querySingle("SELECT * FROM dbs where name='".$name."' LIMIT 1;", True);
        }

        function getDbByUid($uid) {
            return $this->conn->querySingle("SELECT * FROM dbs where uid='".$uid."' LIMIT 1;", True);
        }

        function dbExists($uid) {
            return (!empty($this->getDbByUid($uid))) ? True : False;
        }

        
    }

    class rethinkLite extends meta {
        protected $conn;
        protected $db_name;
        protected $table_name;
        protected $docs;
        protected $query = null;

        function __construct($database=null) {
            parent::__construct();
            if($database != null) {
                $this->dbMeta = $this->getDbByName($database);
                $this->set_db($database);
            }
            return $this;
        }
        
        function createTable($name) {
            if(!$this->dbExists($this->dbMeta['uid'])) {
                return $this->setError(2002);
            }

            if(!empty($this->getTableByName($name, $this->dbMeta['uid']))) {
                return $this->setError(2003);
            }

            $uid = $this->gen_uid();
            if(file_exists(DB_PATH.$uid)) {
                return $this->setError(2004);
            }
            
            $this->conn->exec("INSERT INTO tables ('uid', 'db_uid', 'name') VALUES ('".$uid."', '".$this->dbMeta['uid']."', '".$name."');");
            if($this->conn->changes() == 1) {
                (new SQLite3(DB_PATH.$uid.".db"))->exec("CREATE TABLE table_data (uid varchar(35) primary key, doc text);");
                return $uid;
            }
            return False;
        }

        function createDatabase($name) {
            if(!empty($this->getDbByName($name))) {
                return $this->setError(2000);
            }

            $uid = $this->gen_uid();
            $this->conn->exec("INSERT INTO dbs ('uid', 'name') VALUES ('".$uid."', '".$name."');");
            if($this->conn->changes() == 1) {
                return $uid;
            }
            return $this->setError(2001);
        }

        function set_db($database) {
            $this->db_name = $database;
            return $database;
        }

        function set_table_name($table_name) {
            $this->table_name = $table_name;
            return $table_name;
        }

        function connect() {
            $this->conn = new SQLite3(DB_PATH.$this->tableMeta['uid'].'.db');
            return $this->conn;
        }
        
        function run() {
            if($this->query == null) {
                $results = $this->conn->query("SELECT * FROM table_data;");
            } else {
                $results = array();
            }
            return (new results($results, array('conn' => $this->conn)));
        }
        
        function table($name) {
            $this->tableMeta = $this->getTableByName($name, $this->dbMeta['uid']);
            $this->connect();
            return $this;
        }

        function insert($docs) {
            if(isset($this->tableMeta['uid'])) {
                if (!empty($docs)) {
                    $this->setError(2006);
                }
                
                if (is_assoc($docs)) {
                    $docs = array($docs);
                }
                    
                foreach($docs as $dex=>$dat) {
                    $doc = null; $uid = null;
                    
                    if(isset($dat['_id']) && ( !is_bool($dat['_id']) || !is_array($dat['_id'])) ) {
                         $uid = (string)$dat['_id'];
                         unset($dat['_id']);
                    } else {
                        $uid = $this->gen_uid();
                    }
                    
                    $doc = json_encode($dat);
                    $this->conn->exec("INSERT INTO table_data ('uid', 'doc') VALUES ('".$uid."', '".$doc."');");
                    
                    if(!empty($this->conn->changes())) {
                        $uids[] = $uid;
                    }
                }
                
                if(!empty($uids)) {
                    return $uids;
                }
               
                return $this->setError(2005);
            }
            return false;
        }
        
        function get($uid) {
            $doc = array(0, new document($this->conn->querySingle("SELECT * FROM ".$this->table_name." WHERE uid='".$uid."' LIMIT 1;", True)));
        }

        function getAll() {
        }

        protected function get_error_list() {
            return array(
                2000 => 'Database already exists',
                2001 => 'Failed to create database',
                2002 => 'Database does not exist',
                2003 => 'Table already exists in DB',
                2004 => 'Table file already exists',
                2005 => 'No values inserted',
                2006 => 'No values were passed to be inserted',
            );
        }
    }
    
    class results {
        protected $docs = array();
        
        function __construct($results, $opt) {
            if(count($results) > 0) {
                while($doc = $results->fetchArray(SQLITE3_ASSOC)) {
                    $doc['doc'] = json_decode($doc['doc'], True);
                    $this->docs[] = new document($doc);
                }
                
                if(isset($opt['conn'])) {
                    $this->conn =& $opt['conn'];
                }
            }
            return $this;
        }
        
        function filter($func) {
            foreach($this->docs as $dex=>$dat) {
                $result = $func($dat);
                if($result == FALSE) {
                    unset($this->docs[$dex]);
                }
                elseif(is_object($result) && get_class($result) == 'document') {
                    $this->docs[$dex] = $result;
                }
            }
            $this->fix_keys();
            return $this;
        }
        
        function pluck($keys) {
            if(is_array($keys) && !empty($keys)) {
                $func = function($doc) use ($keys) {
                    foreach($keys as $key) {
                        $new_doc[$key] = $doc($key)->toNative();
                    }
                    return new document(array('uid'=> $doc('_id')->toNative(), 'doc' => $new_doc));
                };
            }
            
            return $this->filter($func);
        }
        
        function update($change, $opt=null) {
            $opt = (!is_null($opt)) ? $opt : array();
            foreach($this->docs as $dat) {
                if(is_closure($change)) {
                    $jsonDat = $change($dat->doc);
                } else {
                    $jsonDat = array_merge($dat->doc, $change);
                }
                unset($jsonDat['_id']);
                $this->updateDocByUid($dat->uid, $jsonDat); 
            }
        }
        
        function replace() {
            
        }
        
        private function updateDocByUid($uid, $doc) {
            $this->conn->exec("UPDATE table_data SET doc='".json_encode($jsonDat)."'");
            return (bool) $this->conn->changes();
        }
        
        function delete() {
            $uids = $this->get_uids();
            if(!empty($uids)) {
                $this->conn->exec("DELETE FROM table_data WHERE uid IN ('".implode("','", $uids)."');");
                return $this->conn->changes();
            }
            return 0;
        }
        
        function commit() {
            
        }
        
        private function get_uids() {
            $uids = array();
            if(!empty($this->docs)) foreach($this->docs as &$dat) {
                $uids[] = $dat->get_uid();
            }
            return $uids;
        }
        
        function fix_keys() {
            $this->docs = array_values($this->docs);
        }

        function export() {
            return var_export($this->docs, True);
        }
    }

    class document {
        private $uid = null;
        private $doc = null;

        function __construct($args) {
            extract($args, EXTR_OVERWRITE);
            $this->uid = (!isset($uid)) ? null : $uid;
            $this->doc =  $doc;
            
            if($this->uid != null) { //set the uid in the doc its self
                $this->doc['_id'] =& $this->uid;
            }
            
            return True;
        }
        
        function get_doc() { //returns the doc
            return $this->doc;
        }
        
        function get_uid() {
            return $this->uid;
        }

        function field($field) {
            return new document(array('uid' => null, 'doc' => $this->doc[$field]));
        }

        function nth($num) {
            return new document(array('uid' => null, 'doc' => $this->doc[$num]));
        }

        function __toString() {
            return $this->doc;
        }
        
        function toNative() {
            return $this->doc;
        }

        function __invoke($field) {
            return $this->field($field);
        }
        
        /*function map($cb) {
            return new document(array('doc' => ($this->doc/$num)));
        }
        
        function add($val) {
            if(is_numeric($val) && !is_string($val)) {
                return new document(array('doc' => ($this->doc+$val)));
            }
            
            if(is_array($val)) {
                
            }
        }*/
        
        function div($num) {
            return new document(array('doc' => ($this->doc/$num)));
        }

        function mod($num) {
            return new document(array('doc' => ($this->doc%$num)));
        }

        function eq($val) {
            if($this->doc == $val) {
                return True;
            } else { return False; }
        }
        
        function ne($val) {
            if($this->doc != $val) {
                return True;
            } else { return False; }
        }
    }

//$d = (new rethinkLite())->createDatabase('testing'); 
//$d = (new rethinkLite('testing'))->createTable('table1'); 
$d = (new rethinkLite('testing'))->table('table1')->insert($_SERVER); 
//var_dump($d);
//exit();
$f = (new rethinkLite('testing'))->table("table1")->run()->filter(function($doc) {
    if($doc("REQUEST_TIME")->mod(2)->eq(0)) {
        return True;
    }
    return False;
})->pluck(array('REQUEST_TIME'))->export();
print_r($f);

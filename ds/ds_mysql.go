package ds

const Create_dh_dp_mysql string = `CREATE TABLE IF NOT EXISTS
    DH_DP (
       DPID    INTEGER PRIMARY KEY AUTO_INCREMENT,
       DPNAME  VARCHAR(32),
       DPTYPE  VARCHAR(32),
       DPCONN  VARCHAR(256),
       STATUS  CHAR(2)
    );`

const Create_dh_dp_repo_ditem_map_mysql string = `CREATE TABLE IF NOT EXISTS
    DH_DP_RPDM_MAP (
    	RPDMID       INTEGER PRIMARY KEY AUTO_INCREMENT,
        REPOSITORY   VARCHAR(128),
        DATAITEM     VARCHAR(128),
        DPID         INTEGER,
        ITEMDESC     VARCHAR(256),
        PUBLISH      CHAR(2),
        CREATE_TIME  DATETIME,
        STATUS       CHAR(2)
    );`

const Create_dh_repo_ditem_tag_map_mysql string = `CREATE TABLE IF NOT EXISTS
    DH_RPDM_TAG_MAP (
    	TAGID        INTEGER PRIMARY KEY AUTO_INCREMENT,
        TAGNAME      VARCHAR(128),
        RPDMID       INTEGER,
        DETAIL       VARCHAR(256),
        CREATE_TIME  DATETIME,
        STATUS       CHAR(2),
        COMMENT		 VARCHAR(256)
    );`

const CreateDhDaemon_mysql string = `CREATE TABLE IF NOT EXISTS
    DH_DAEMON (
    	DAEMONID       VARCHAR(64),
        ENTRYPOINT     VARCHAR(128),
        STATUS         CHAR(2)
    );`

const CreateDhJob_mysql string = `CREATE TABLE IF NOT EXISTS
    DH_JOB (
    	JOBID 	VARCHAR(32),
        TAG		VARCHAR(256),
        FILEPATH	VARCHAR(256),
        STATUS		VARCHAR(20),
        CREATE_TIME	DATETIME,
        STAT_TIME	DATETIME,
        DOWNSIZE	BIGINT,
        SRCSIZE		BIGINT,
        ACCESSTOKEN VARCHAR(20),
        ENTRYPOINT  VARCHAR(128)
    );`

const CreateMsgTagAdded_mysql string = `CREATE TABLE IF NOT EXISTS
	MSG_TAGADDED (
		ID 			INTEGER PRIMARY KEY AUTO_INCREMENT,
		REPOSITORY  VARCHAR(128) NOT NULL,
		DATAITEM    VARCHAR(128) NOT NULL,
		TAG			VARCHAR(128) NOT NULL,
		STATUS	    INT,
		CREATE_TIME DATETIME,
		STATUS_TIME DATETIME
	);`

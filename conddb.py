import re
import cx_Oracle

class CondDB(object):
    """
    Simple oracle wrapper with interfaces to ECALDQM-specific functions.
    """

    class DBRow(object):
        def __init__(self, descriptions_, row_):
            self.values = {}
            if row_ is None:
                return

            for iC in range(len(row_)):
                self.values[descriptions_[iC][0]] = row_[iC]


    def __init__(self, dbName_, user_, password_):
        self._conn = cx_Oracle.connect(user_ + '/' + password_ + '@' + dbName_)
        self._cur = self._conn.cursor()

    def close(self):
        self._cur.close()
        self._conn.close()

    def getOneRow(self, query_, **kwargs):
        try:
            self._cur.execute(query_, **kwargs)
        except cx_Oracle.DatabaseError:
            return CondDB.DBRow(None, None)

        return CondDB.DBRow(self._cur.description, self._cur.fetchone())

    def getAllRows(self, query_, **kwargs):
        self._cur.execute(query_, **kwargs)
        rows = []
        row = self._cur.fetchone()
        while row:
            rows.append(CondDB.DBRow(self._cur.description, row))
            row = self._cur.fetchone()
        return rows


class EcalCondDB(CondDB):
    """
    Oracle wrapper with interfaces to ECALDQM-specific functions.
    """

    def __init__(self, dbName_, user_, password_):
        CondDB.__init__(self, dbName_, user_, password_)

    def getNewRunNumber(self, minRun_ = 0, location_ = 'P5_Co'):
        row = self.getOneRow('\
        SELECT RUN_NUM FROM\
        RUN_IOV\
        INNER JOIN RUN_TAG ON RUN_IOV.TAG_ID = RUN_TAG.TAG_ID\
        WHERE\
        RUN_IOV.RUN_NUM > :run\
        AND RUN_IOV.RUN_START > SYSDATE - 1\
        AND RUN_IOV.RUN_END > SYSDATE\
        AND RUN_TAG.LOCATION_ID IN (\
          SELECT DEF_ID FROM LOCATION_DEF WHERE LOCATION LIKE :loc\
        )',
        run = minRun_, loc = location_)

        if len(row.values) == 0:
            return 0
        else:
            return row.values['RUN_NUM']

    def getRunIOV(self, run_, location_ = 'P5_Co'):
        return self.getOneRow('\
            SELECT * FROM\
            RUN_IOV\
            INNER JOIN RUN_TAG ON RUN_IOV.TAG_ID = RUN_TAG.TAG_ID\
            WHERE\
            RUN_IOV.RUN_NUM = :run\
            AND RUN_TAG.LOCATION_ID IN (\
              SELECT DEF_ID FROM LOCATION_DEF WHERE LOCATION LIKE :loc\
            )',
            run = run_, loc = location_)
        
    def getMonRunIOV(self, runIOV_):
        return self.getOneRow('SELECT * FROM MON_RUN_IOV WHERE RUN_IOV_ID = :runiov AND SUBRUN_NUM = 1', runiov = runIOV_.values['IOV_ID'])

    def insertMonRunIOV(self, runIOV_):
        # these parameters are variable in principle but fixed in practice
        monRunTag = self.getOneRow('SELECT * FROM MON_RUN_TAG WHERE GEN_TAG LIKE \'CMSSW-offline-private\' AND MON_VER_ID = 1')
        if len(monRunTag.values) == 0:
            raise RuntimeError("MonRun tag not found in DB")
            
        self._cur.execute('INSERT INTO MON_RUN_IOV (IOV_ID, TAG_ID, RUN_IOV_ID, SUBRUN_NUM, SUBRUN_START, SUBRUN_END)\
        VALUES\
        (mon_run_iov_sq.NextVal, :tag, :runiov, 1, :stt, :edt)',
                          tag = monRunTag.values['TAG_ID'],
                          runiov = runIOV_.values['IOV_ID'],
                          stt = runIOV_.values['RUN_START'],
                          edt = runIOV_.values['RUN_END'])
            
    def setMonRunOutcome(self, run_, outcome_, location_ = 'P5_Co'):
        # get the outcome definition
        outcomeDef = self.getOneRow('SELECT * FROM MON_RUN_OUTCOME_DEF WHERE SHORT_DESC LIKE :sd', sd = outcome_)
        if len(outcomeDef.values) == 0:
            raise RuntimeError("Outcome " + outcome_ + " not defined")
        
        # get the DAQ RunIOV for the given run
        runIOV = self.getRunIOV(run_, location_)
        if runIOV is None:
            raise RuntimeError("Run IOV not found in DB")

        # get the DQM RunIOV for the given run (subrun is fixed to 1)
        monRunIOV = self.getMonRunIOV(runIOV)
        
        if len(monRunIOV.values) == 0:
            # need to create a new DQM RunIOV
            self.insertMonRunIOV(runIOV)
            monRunIOV = self.getMonRunIOV(runIOV)

        # History:
        #  Initially one MON_RUN_DAT entry per run was created with LOGIC_ID = 1.
        #  Later, the DQM job was split into EB and EE, and the DB writing accordingly, so there were two entries per run with LOGIC_ID = 1000000000 and 2000000000.
        #  Even after the EB and EE DQM were merged, we kept writing two entries per run until 2014. The two rows were complete duplicate of each other.
        #  Since April 2014 we are back to writing only one entry per run with LOGIC_ID = 1.
        monRunDat = self.getOneRow('SELECT * FROM MON_RUN_DAT WHERE IOV_ID = :id', id = monRunIOV.values['IOV_ID'])
        if len(monRunDat.values) == 0:
            self._cur.execute('INSERT INTO MON_RUN_DAT (IOV_ID, LOGIC_ID) VALUES (:id, 1)', monRunIOV.values['IOV_ID'])

        elif monRunDat.values['RUN_OUTCOME_ID'] != outcomeDef.values['DEF_ID']:
            self._cur.execute('UPDATE MON_RUN_DAT SET RUN_OUTCOME_ID = :outcome WHERE IOV_ID = :iov', outcome = outcomeDef.values['DEF_ID'], iov = monRunIOV.values['IOV_ID'])


class RunParameterDB(CondDB):
    """
    Oracle wrapper with interfaces to WBM-specific functions.
    """

    def __init__(self, dbName_, user_, password_):
        CondDB.__init__(self, dbName_, user_, password_)

    def getLatestRun(self):
        row = self.getOneRow('\
        SELECT MAX(RUNNUMBER) FROM\
        CMS_RUNINFO.RUNSESSION_PARAMETER\
        ')

        if len(row.values) == 0:
            return 0
        else:
            return row.values['MAX(RUNNUMBER)']

    def getRunParameter(self, run, paramName):
        row = self.getOneRow('\
        SELECT STRING_VALUE FROM\
        CMS_RUNINFO.RUNSESSION_PARAMETER\
        WHERE\
        RUNNUMBER = :run\
        AND NAME LIKE :name\
        ',
        run = run, name = paramName)

        if len(row.values) == 0:
            return ''
        else:
            return row.values['STRING_VALUE']

    def getDAQType(self, run):
        matches = re.match('/global_configuration_map/cms/(central|minidaq)/', self.getRunParameter(run, 'CMS.LVL0:GLOBAL_CONF_KEY').lower())
        if matches:
            return matches.group(1)
        else:
            return ''
        
        
if __name__ == '__main__': # FOR DEBUGGING

    from ecaldqmconfig import config

    db = RunParameterDB(config.dbread.dbName, config.dbread.dbUserName, config.dbread.dbPassword)

    print db.getLatestRun()

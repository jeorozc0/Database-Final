import unittest
import datetime
from mysql_connector import mysql_connector_one as connector
from mydao import data_access_object as dao

class TestClass( unittest.TestCase ):
    
    def testInsertBatch(self):
        con = connector.connection()
        json_data = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"static_data","IMO":"Unknown","Name":"WIND FARM BALTIC1NW","VesselType":"Undefined","Length":60,"Breadth":60,"A":30,"B":30,"C":30,"D":30},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111923,"MsgType":"static_data","IMO":"Unknown","Name":"BALTIC2 WINDFARM SW","VesselType":"Undefined","Length":8,"Breadth":12,"A":4,"B":4,"C":4,"D":8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        results = dao.insertBatchMessages(con, json_data)
        connector.end_connection(con)
        self.assertEqual(results, '10')
    
    def testInsertBatchWrongType(self):
        con = connector.connection()
        data = 4
        results = dao.insertBatchMessages(con, data)
        connector.end_connection(con)
        self.assertEqual(results, '0')
    
    def testIndividualPosition(self):
        con = connector.connection()
        json_data = {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97}
        results = dao.insertIndividualMessage(con, json_data)
        connector.end_connection(con)
        self.assertEqual(results, '1')

    def testIndividualStatic(self):
        con = connector.connection()
        json_data = {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":210169000,"MsgType":"static_data","IMO":9584865,"CallSign":"5BNZ3","Name":"KATHARINA SCHEPERS","VesselType":"Cargo","CargoType":"Category X","Length":152,"Breadth":24,"Draught":7.8,"Destination":"NODRM","ETA":"2020-11-18T09:00:00.000Z","A":143,"B":9,"C":13,"D":11}
        results = dao.insertIndividualMessage(con, json_data)
        connector.end_connection(con)
        self.assertEqual(results, '1')

    def testIndividualWrongType(self):
        con = connector.connection()
        data = "HELLO"
        results = dao.insertIndividualMessage(con, data)
        connector.end_connection(con)
        self.assertEqual(results, '0')

    def testDeleteOldMessage(self):
        json_data = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":210169000,"MsgType":"static_data","IMO":9584865,"CallSign":"5BNZ3","Name":"KATHARINA SCHEPERS","VesselType":"Cargo","CargoType":"Category X","Length":152,"Breadth":24,"Draught":7.8,"Destination":"NODRM","ETA":"2020-11-18T09:00:00.000Z","A":143,"B":9,"C":13,"D":11},
        {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97}]
        con = connector.connection()
        dao.insertBatchMessages(con, json_data)
        results = dao.deleteOldMessages(con)
        connector.end_connection(con)
        self.assertEqual(results, '2')

    def testDeleteAllButOne(self):
        timestamp = datetime.datetime.strptime(str(datetime.datetime.now()), "%Y-%m-%d %H:%M:%S.%f")
        converted = datetime.datetime.strftime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        finalTime = converted[:23] + "Z"
        json_data = [{"Timestamp": finalTime, "Class":"Class A","MMSI":210169000,"MsgType":"static_data","IMO":9584865,"CallSign":"5BNZ3","Name":"KATHARINA SCHEPERS","VesselType":"Cargo","CargoType":"Category X","Length":152,"Breadth":24,"Draught":7.8,"Destination":"NODRM","ETA":"2020-11-18T09:00:00.000Z","A":143,"B":9,"C":13,"D":11},
        {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":210169000,"MsgType":"static_data","IMO":9584865,"CallSign":"5BNZ3","Name":"KATHARINA SCHEPERS","VesselType":"Cargo","CargoType":"Category X","Length":152,"Breadth":24,"Draught":7.8,"Destination":"NODRM","ETA":"2020-11-18T09:00:00.000Z","A":143,"B":9,"C":13,"D":11},
        {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97}]
        con = connector.connection()
        dao.insertBatchMessages(con, json_data)
        results = dao.deleteOldMessages(con)
        connector.end_connection(con)
        self.assertEqual(results, '1')

    def testGetAllPositions(self):
        json_data = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        con = connector.connection()
        dao.insertBatchMessages(con, json_data)
        results = dao.readAllRecentPositions(con)
        connector.end_connection(con)
        self.assertEqual(results, [
            {'MMSI': 229964000, 'lat': 54.664513, 'long': 13.068712, 'IMO': 7106877, 'Name': 'Rs Sentinel'},
            {'MMSI': 376503000, 'lat': 54.519373, 'long': 11.47914, 'IMO': 7818066, 'Name': 'Cooler Bay'},
            {'MMSI': 304858000, 'lat': 55.218332, 'long': 13.371672, 'IMO': 8214358, 'Name': 'St.Pauli'},
            {'MMSI': 257385000, 'lat': 55.219403, 'long': 13.127725, 'IMO': 8813972, 'Name': 'Kegums'},
            {'MMSI': 219570000, 'lat': 55.07848, 'long': 12.814233, 'IMO': 8862569, 'Name': 'Soloven'},
            {'MMSI': 376503000, 'lat': 54.519373, 'long': 11.47914, 'IMO': 9081356, 'Name': 'Isidor'},
            {'MMSI': 257961000, 'lat': 55.00316, 'long': 12.809015, 'IMO': 9231535, 'Name': 'Normand Cutter'}])

    def testGetAllPostitionsEmpty(self):
        con = connector.connection()
        results = dao.readAllRecentPositions(con)
        connector.end_connection(con)
        self.assertEqual(results, [])

    def testSameValuePosition(self):
        jsonData = [
            {"Timestamp":"2020-11-19T10:10:10.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[53.218332,12.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97}]
        con = connector.connection()
        dao.insertBatchMessages(con,jsonData)
        results = dao.readAllRecentPositions(con)
        connector.end_connection(con)
        self.assertEqual(results, [{"MMSI": 304858000,"lat": 53.218332,"long": 12.371672, "IMO": 8214358, "Name": 'St.Pauli'}])

    def testReadMostRecentPosition(self):
        jsonData = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        con = connector.connection()
        dao.insertBatchMessages(con,jsonData)
        mmsi = 304858000
        results = dao.readOneRecentPosition(con, mmsi)
        self.assertEqual(results, '{"MMSI": 304858000, "lat": 55.218332, "long": 13.371672, "IMO": 8214358, "Name": "St.Pauli"}')

    def testReadMostRecentPositionWrongType(self):
        jsonData = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        con = connector.connection()
        dao.insertBatchMessages(con,jsonData)
        mmsi = "HELLO"
        results = dao.readOneRecentPosition(con, mmsi)
        connector.end_connection(con)
        self.assertEqual(results, '{}')

    def testReadMostRecentWithSameMMSI(self):
        jsonData = [
            {"Timestamp":"2020-11-19T10:10:10.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[53.218332,12.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97}]
        con = connector.connection()
        dao.insertBatchMessages(con,jsonData)
        mmsi = 304858000
        results = dao.readOneRecentPosition(con, mmsi)
        connector.end_connection(con)
        self.assertEqual(results, '{"MMSI": 304858000, "lat": 53.218332, "long": 12.371672, "IMO": 8214358, "Name": "St.Pauli"}')

    def testShipPositionToPort(self):
        jsonData = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        con = connector.connection()
        dao.insertBatchMessages(con, jsonData)
        portName = "Frederikshavn"
        country = "Denmark"
        results = dao.readShipPositionToPort(con, portName, country)
        connector.end_connection(con)
        self.assertEqual(results, [{'IMO': 7106877, 'MMSI': 229964000, 'lat': 54.664513, 'long': 13.068712},
            {'IMO': 7818066, 'MMSI': 376503000, 'lat': 54.519373, 'long': 11.47914},
            {'IMO': 8214358, 'MMSI': 304858000, 'lat': 55.218332, 'long': 13.371672},
            {'IMO': 8813972, 'MMSI': 257385000, 'lat': 55.219403, 'long': 13.127725},
            {'IMO': 8862569, 'MMSI': 219570000, 'lat': 55.07848, 'long': 12.814233},
            {'IMO': 9081356, 'MMSI': 376503000, 'lat': 54.519373, 'long': 11.47914},
            {'IMO': 9231535, 'MMSI': 257961000, 'lat': 55.00316, 'long': 12.809015}])

    def testshipPositionToPortMultiplePorts(self):
        jsonData = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        con = connector.connection()
        dao.insertBatchMessages(con, jsonData)
        portName = "Samso Island"
        country = "Denmark"
        results = dao.readShipPositionToPort(con, portName, country)
        connector.end_connection(con)
        self.assertEqual(results, [{"Id": 2998,"Name": "Samso Island","Country": "Denmark","lat": 55.795833,"long": 10.528611,"scale_1": 1,"scale_2": 5332,"scale_3": 53322},
        {"Id": 2999,"Name": "Samso Island","Country": "Denmark","lat": 55.863611,"long": 10.548611,"scale_1": 1,"scale_2": 5332,"scale_3": 53322}])
    
    def testShipPositionToPortWrongType(self):
        jsonData = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        con = connector.connection()
        dao.insertBatchMessages(con, jsonData)
        portName = 324234
        country = "Denmark"
        results = dao.readShipPositionToPort(con, portName, country)
        connector.end_connection(con)
        self.assertEqual(results, [])

    def testLastFivePositions(self):
        jsonData = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-12-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[56.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-19T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,11.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2021-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,12.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2022-01-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[56.218332,13.571672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2021-03-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[57.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-25T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-12-02T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.218332,12.541672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-10-08T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        con = connector.connection()
        dao.insertBatchMessages(con, jsonData)
        mmsi = 304858000
        result = dao.readLastFivePositions(con, mmsi)
        connector.end_connection(con)
        self.assertEqual(result, '{"MMSI": 304858000, "Positions": [{"lat": 56.218332, "long": 13.571672}, {"lat": 55.218332, "long": 12.371672}, {"lat": 57.218332, "long": 13.371672}, {"lat": 56.218332, "long": 13.371672}, {"lat": 54.218332, "long": 12.541672}], "IMO": 8214358}')

    def testLastFivePositionsWrongType(self):
        jsonData = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
            {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        con = connector.connection()
        dao.insertBatchMessages(con, jsonData)
        mmsi = "Hello"
        result = dao.readLastFivePositions(con, mmsi)
        connector.end_connection(con)
        self.assertEqual(result, '{}')

    def testRetrievePortNoCountry(self):
        con = connector.connection()
        port_location = "Frederikshavn"
        country = None
        results = dao.getPortByName(con,port_location,country)
        connector.end_connection(con)
        self.assertEqual(results,[{'Id': 1221, 'Name': 'Frederikshavn', 'Country': 'Denmark', 'Latitude': 57.437778, 'Longitude': 10.546111, 'Map View 1': 1, 'Map View 2': 5335, 'Map View 3': 53352}])

    def testRetrievePortsCountry(self):
        con = connector.connection()
        port_location = "Copenhagen"
        country = "Denmark"
        results = dao.getPortByName(con,port_location,country)
        connector.end_connection(con)
        self.assertEqual(results,[{'Id': 541, 'Name': 'Copenhagen', 'Country': 'Denmark', 'Latitude': 55.696111, 'Longitude': 12.613333, 'Map View 1': 1, 'Map View 2': 5528, 'Map View 3': 55284}]) 

    def testRetrievePortInvalidLocationType(self):
        con = connector.connection()
        port_location = 121
        country = None
        results = dao.getPortByName(con,port_location,country)
        connector.end_connection(con)
        self.assertEqual(results,[])

    def testRetrievePortInvalidcountryType(self):
        con = connector.connection()
        port_location = "Copenhagen"
        country = 121
        results = dao.getPortByName(con,port_location,country)
        connector.end_connection(con)
        self.assertEqual(results,[])
       

    def testShipPositionsInTile(self):
        con = connector.connection()
        json_data = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"static_data","IMO":"Unknown","Name":"WIND FARM BALTIC1NW","VesselType":"Undefined","Length":60,"Breadth":60,"A":30,"B":30,"C":30,"D":30},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111923,"MsgType":"static_data","IMO":"Unknown","Name":"BALTIC2 WINDFARM SW","VesselType":"Undefined","Length":8,"Breadth":12,"A":4,"B":4,"C":4,"D":8},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        dao.insertBatchMessages(con, json_data)
        
        results = dao.ShipPositionsInTile(con, 1)
        
        connector.end_connection(con)
        self.assertEqual(results,[{'MMSI': 229964000, 'Latitude': 54.664513, 'Longitude': 13.068712, 'IMO': 7106877, 'Name': 'Rs Sentinel'}, {'MMSI': 376503000, 'Latitude': 54.519373, 'Longitude': 11.47914, 'IMO': 7818066, 'Name': 'Cooler Bay'}, {'MMSI': 304858000, 'Latitude': 55.218332, 'Longitude': 13.371672, 'IMO': 8214358, 'Name': 'St.Pauli'}, {'MMSI': 257385000, 'Latitude': 55.219403, 'Longitude': 13.127725, 'IMO': 8813972, 'Name': 'Kegums'}, {'MMSI': 219570000, 'Latitude': 55.07848, 'Longitude': 12.814233, 'IMO': 8862569, 'Name': 'Soloven'}, {'MMSI': 376503000, 'Latitude': 54.519373, 'Longitude': 11.47914, 'IMO': 9081356, 'Name': 'Isidor'}, {'MMSI': 257961000, 'Latitude': 55.00316, 'Longitude': 12.809015, 'IMO': 9231535, 'Name': 'Normand Cutter'}])

    def testShipPositionsInTileInvalideTileId_type(self):
        con = connector.connection()
        json_data = [{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"static_data","IMO":"Unknown","Name":"WIND FARM BALTIC1NW","VesselType":"Undefined","Length":60,"Breadth":60,"A":30,"B":30,"C":30,"D":30},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219005465,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.572602,11.929218]},"Status":"Under way using engine","RoT":0,"SoG":0,"CoG":298.7,"Heading":203},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257961000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.00316,12.809015]},"Status":"Under way using engine","RoT":0,"SoG":0.2,"CoG":225.6,"Heading":240},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111923,"MsgType":"static_data","IMO":"Unknown","Name":"BALTIC2 WINDFARM SW","VesselType":"Undefined","Length":8,"Breadth":12,"A":4,"B":4,"C":4,"D":8},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":257385000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.219403,13.127725]},"Status":"Under way using engine","RoT":25.7,"SoG":12.3,"CoG":96.5,"Heading":101},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":376503000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.519373,11.47914]},"Status":"Under way using engine","RoT":0,"SoG":7.6,"CoG":294.4,"Heading":290},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":229964000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.664513,13.068712]},"Status":"Under way using engine","RoT":0,"SoG":9.3,"CoG":68.2,"Heading":71},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":219570000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.07848,12.814233]},"Status":"Under way using engine","SoG":0.8,"CoG":65.8},
{"Timestamp":"2020-11-18T00:00:00.000Z","Class":"AtoN","MMSI":992111840,"MsgType":"position_report","Position":{"type":"Point","coordinates":[54.61291,12.62997]},"Status":"Unknown value"}]
        dao.insertBatchMessages(con, json_data)

        results = dao.ShipPositionsInTile(con, "hi")

        connector.end_connection(con)

        self.assertEqual(results,[])

    def testVesselInformationWithStaticData(self):
        con = connector.connection()
        jsonData = {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":235095435,"MsgType":"static_data","IMO":1000019,"CallSign":"5BNZ3","Name":"Lady K Ii","VesselType":"Cargo","CargoType":"Category X","Length":152,"Breadth":24,"Draught":7.8,"Destination":"NODRM","ETA":"2020-11-18T09:00:00.000Z","A":143,"B":9,"C":13,"D":11}
        dao.insertIndividualMessage(con,jsonData)
        mmsi = 235095435
        imo = 1000019
        name = "Lady K Ii"
        callsign = None
        results = dao.readVesselInformationByCriteria(con, mmsi, imo, name, callsign)
        connector.end_connection(con)
        self.assertEqual(results, """{"IMO": 1000019, "Flag": "United Kingdom", "Name": "Lady K Ii", "Built": 1961, "Callsign": null, "Lenght": 57, "Breadth": 8, "Tonage": 551, "MMSI": 235095435, "Type": "Yacht", "Status": "Active", "Order": 1, "CallSign": "5BNZ3", "Vessel_Type": "Cargo", "Cargo_Type": "Category X", "Length": 152, "Draught": 7.8, "AIS_Destination": "NODRM", "ETA": "2020-11-18 09:00:00", "A": 143, "B": 9, "C": 13, "D": 11}""")

    def testVesselInformationWithOnlyVesselObject(self):
        con = connector.connection()
        mmsi = 235095435
        imo = None
        name = None
        callsign = None
        results = dao.readVesselInformationByCriteria(con, mmsi, imo, name, callsign)
        connector.end_connection(con)
        self.assertEqual(results, """{"IMO": 1000019, "Flag": "United Kingdom", "Name": "Lady K Ii", "Built": 1961, "Callsign": null, "Lenght": 57, "Breadth": 8, "Tonage": 551, "MMSI": 235095435, "Type": "Yacht", "Status": "Active", "Order": 1}""")

    def testVesselInformationWithIncorrectDataType(self):
        con = connector.connection()
        mmsi = 'HELLO'
        imo = None
        name = None
        callsign = None
        results = dao.readVesselInformationByCriteria(con, mmsi, imo, name, callsign)
        connector.end_connection(con)
        self.assertEqual(results, """{}""")

def main():
    unittest.main()

if __name__ == '__main__':
    main()
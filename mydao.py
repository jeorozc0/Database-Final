from mysql_connector import mysql_connector_one as connector
import json as importedJson
import datetime

class data_access_object:
    def insertBatchMessages(con, inputData):
        """Inserts new AIS messages into the database.
        :param con: A connection object.
        :type con: mysq.connector.connection.MysqlConnection
        :param inputData: List of JSON AIS messages to be inserted into the database.
        :type inputData: List
        :return: The number of messages inserted into the database.
        :rtype: JSON String
        """
        if(type(inputData) != list):
            return importedJson.dumps(0)

        count = 0

        for json in inputData:
            try:
                setToJson = importedJson.loads(importedJson.dumps(json))
                if "Timestamp" in setToJson:
                    timeStamp = setToJson["Timestamp"]
                    convertToDatetime = datetime.datetime.strptime(timeStamp, '%Y-%m-%dT%H:%M:%S.%fZ')
                    convertedTime = "'" + convertToDatetime.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
                else:
                    convertedTime = "NULL"
                if "Class" in setToJson:
                    vesselClass = "'" + setToJson["Class"] + "'"
                else:
                    vesselClass = "NULL" 
                if "MMSI" in setToJson:
                    mmsi = setToJson["MMSI"]
                else:
                    mmsi = "NULL" 
                if "MsgType" in setToJson:
                    messageType = "'" + setToJson["MsgType"] + "'"
                else:
                    messageType = "NULL" 
                aisQuery = "INSERT INTO AIS_MESSAGE(AIS_Timestamp, Class, MMSI, MsgType) VALUES({}, {}, {}, {});".format(convertedTime, vesselClass, mmsi, messageType)
                connector.run(con, aisQuery)
                
                if(messageType == "'position_report'"):
                    lastAis = "SELECT LAST_INSERT_ID() FROM AIS_MESSAGE;"
                    getId = connector.run(con, lastAis)
                    positionId = getId[0][0]
                    if "Position" in setToJson:
                        position = setToJson["Position"]
                        if "type" in position:
                            positionType = "'" + position["type"] + "'"
                        else:
                            positionType = "NULL"
                        if "coordinates" in position:
                            latitude = position["coordinates"][0]
                            longitude = position["coordinates"][1]
                    else:
                        position = "NULL" 
                    if "Status" in setToJson:
                        status = "'" + setToJson["Status"] + "'"
                    else:
                        status = "NULL" 
                    if "RoT" in setToJson:
                        rot = setToJson["RoT"]
                    else:
                        rot = "NULL" 
                    if "SoG" in setToJson:
                        sog = setToJson["SoG"]
                    else:
                        sog = "NULL" 
                    if "CoG" in setToJson:
                        cog = setToJson["CoG"]
                    else:
                        cog = "NULL" 
                    if "Heading" in setToJson:
                        heading = setToJson["Heading"]
                    else:
                        heading = "NULL"
                    positionQuery = "INSERT INTO POSITION_REPORT(Id, Type, Latitude, Longitude, Status, RoT, SoG, CoG, Heading) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {});".format(positionId, positionType, latitude, longitude, status, rot, sog, cog, heading)
                    connector.run(con, positionQuery)
                    count+=1

                elif(messageType == "'static_data'"):
                    lastAis = "SELECT LAST_INSERT_ID() FROM AIS_MESSAGE;"
                    getId = connector.run(con, lastAis)
                    staticId = getId[0][0]
                    if "IMO" in setToJson and setToJson["IMO"] != "Unknown":
                        vesselIMO = setToJson["IMO"]
                    else:
                        vesselIMO = "NULL" 
                    if "CallSign" in setToJson:
                        callSign = "'"+setToJson["CallSign"]+"'"
                    else:
                        callSign = "NULL" 
                    if "Name" in setToJson:
                        name = "'" + setToJson["Name"] + "'"
                    else:
                        name = "NULL" 
                    if "VesselType" in setToJson:
                        vesselType = "'" + setToJson["VesselType"] + "'"
                    else:
                        vesselType = "NULL" 
                    if "CargoType" in setToJson:
                        cargoType = "'" + setToJson["CargoType"] + "'"
                    else:
                        cargoType = "NULL" 
                    if "Length" in setToJson:
                        length = setToJson["Length"]
                    else:
                        length = "NULL" 
                    if "Breadth" in setToJson:
                        breadth = setToJson["Breadth"]
                    else:
                        breadth = "NULL" 
                    if "Draught" in setToJson:
                        draught = setToJson["Draught"]
                    else:
                        draught = "NULL" 
                    if "Destination" in setToJson:
                        destination = "'" + setToJson["Destination"] + "'"
                    else:
                        destination = "NULL" 
                    if "ETA" in setToJson:
                        eta = setToJson["ETA"]
                        convertToDatetimeEta = datetime.datetime.strptime(eta, '%Y-%m-%dT%H:%M:%S.%fZ')
                        convertedTimeEta = "'" + convertToDatetimeEta.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
                    else:
                        convertedTimeEta = "NULL" 
                    if "A" in setToJson:
                        a = setToJson["A"]
                    else: 
                        a = "NULL" 
                    if "B" in setToJson:
                        b = setToJson["B"]
                    else: 
                        b = "NULL" 
                    if "C" in setToJson:
                        c = setToJson["C"]
                    else: 
                        c = "NULL" 
                    if "D" in setToJson:
                        d = setToJson["D"]
                    else:
                        d = "NULL"
                    staticQuery = "INSERT INTO STATIC_DATA(Id, IMO, CallSign, Name, Vessel_Type, Cargo_Type, Length, Breadth, Draught, AIS_Destination, ETA, A, B, C, D) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});".format(staticId, vesselIMO, callSign, name, vesselType, cargoType, length, breadth, draught, destination, convertedTimeEta, a, b, c, d)
                    connector.run(con, staticQuery)
                    count+=1
            except:
                pass
        return importedJson.dumps(count)

    def insertIndividualMessage(con, inputJson):
        """Inserts one AIS message into the database.
        :param con: A connection object.
        :type con: mysq.connector.connection.MysqlConnection
        :param inputJson: A single AIS message to be added to the database.
        :type inputData: Dict
        :return: Returns the number 1 for success or 0 for failure.
        :rtype: JSON String
        """
        if type(inputJson) != dict:
            return importedJson.dumps(0)
        
        try:
            setToJson = importedJson.loads(importedJson.dumps(inputJson))
            if "Timestamp" in setToJson:
                timeStamp = setToJson["Timestamp"]
                convertToDatetime = datetime.datetime.strptime(timeStamp, '%Y-%m-%dT%H:%M:%S.%fZ')
                convertedTime = "'" + convertToDatetime.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
            else:
                convertedTime = "NULL"
            if "Class" in setToJson:
                vesselClass = "'" + setToJson["Class"] + "'"
            else:
                vesselClass = "NULL" 
            if "MMSI" in setToJson:
                mmsi = setToJson["MMSI"]
            else:
                mmsi = "NULL" 
            if "MsgType" in setToJson:
                messageType = "'" + setToJson["MsgType"] + "'"
            else:
                messageType = "NULL" 
            aisQuery = "INSERT INTO AIS_MESSAGE(AIS_Timestamp, Class, MMSI, MsgType) VALUES({}, {}, {}, {});".format(convertedTime, vesselClass, mmsi, messageType)
            connector.run(con, aisQuery)
            
            if(messageType == "'position_report'"):
                lastAis = "SELECT LAST_INSERT_ID() FROM AIS_MESSAGE;"
                getId = connector.run(con, lastAis)
                positionId = getId[0][0]
                if "Position" in setToJson:
                    position = setToJson["Position"]
                    if "type" in position:
                        positionType = "'" + position["type"] + "'"
                    else:
                        positionType = "NULL"
                    if "coordinates" in position:
                        latitude = position["coordinates"][0]
                        longitude = position["coordinates"][1]
                else:
                    position = "NULL" 
                if "Status" in setToJson:
                    status = "'" + setToJson["Status"] + "'"
                else:
                    status = "NULL" 
                if "RoT" in setToJson:
                    rot = setToJson["RoT"]
                else:
                    rot = "NULL" 
                if "SoG" in setToJson:
                    sog = setToJson["SoG"]
                else:
                    sog = "NULL" 
                if "CoG" in setToJson:
                    cog = setToJson["CoG"]
                else:
                    cog = "NULL" 
                if "Heading" in setToJson:
                    heading = setToJson["Heading"]
                else:
                    heading = "NULL"
                positionQuery = "INSERT INTO POSITION_REPORT(Id, Type, Latitude, Longitude, Status, RoT, SoG, CoG, Heading) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {});".format(positionId, positionType, latitude, longitude, status, rot, sog, cog, heading)
                connector.run(con, positionQuery)
                return importedJson.dumps(1)

            elif(messageType == "'static_data'"):
                lastAis = "SELECT LAST_INSERT_ID() FROM AIS_MESSAGE;"
                getId = connector.run(con, lastAis)
                staticId = getId[0][0]
                if "IMO" in setToJson and setToJson["IMO"] != "Unknown":
                    vesselIMO = setToJson["IMO"]
                else:
                    vesselIMO = "NULL" 
                if "CallSign" in setToJson:
                    callSign = "'"+setToJson["CallSign"]+"'"
                else:
                    callSign = "NULL" 
                if "Name" in setToJson:
                    name = "'" + setToJson["Name"] + "'"
                else:
                    name = "NULL" 
                if "VesselType" in setToJson:
                    vesselType = "'" + setToJson["VesselType"] + "'"
                else:
                    vesselType = "NULL" 
                if "CargoType" in setToJson:
                    cargoType = "'" + setToJson["CargoType"] + "'"
                else:
                    cargoType = "NULL" 
                if "Length" in setToJson:
                    length = setToJson["Length"]
                else:
                    length = "NULL" 
                if "Breadth" in setToJson:
                    breadth = setToJson["Breadth"]
                else:
                    breadth = "NULL" 
                if "Draught" in setToJson:
                    draught = setToJson["Draught"]
                else:
                    draught = "NULL" 
                if "Destination" in setToJson:
                    destination = "'" + setToJson["Destination"] + "'"
                else:
                    destination = "NULL" 
                if "ETA" in setToJson:
                    eta = setToJson["ETA"]
                    convertToDatetimeEta = datetime.datetime.strptime(eta, '%Y-%m-%dT%H:%M:%S.%fZ')
                    convertedTimeEta = "'" + convertToDatetimeEta.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
                else:
                    convertedTimeEta = "NULL" 
                if "A" in setToJson:
                    a = setToJson["A"]
                else: 
                    a = "NULL" 
                if "B" in setToJson:
                    b = setToJson["B"]
                else: 
                    b = "NULL" 
                if "C" in setToJson:
                    c = setToJson["C"]
                else: 
                    c = "NULL" 
                if "D" in setToJson:
                    d = setToJson["D"]
                else:
                    d = "NULL" 
                staticQuery = "INSERT INTO STATIC_DATA(Id, IMO, CallSign, Name, Vessel_Type, Cargo_Type, Length, Breadth, Draught, AIS_Destination, ETA, A, B, C, D) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});".format(staticId, vesselIMO, callSign, name, vesselType, cargoType, length, breadth, draught, destination, convertedTimeEta, a, b, c, d)
                connector.run(con, staticQuery)
                return importedJson.dumps(1)
        except:
            return importedJson.dumps(0)
    
    def deleteOldMessages(con):
        """Deletes all AIS messages older than 5 minutes.
        :param con: A connection object.
        :type con: mysq.connector.connection.MysqlConnection
        :return: Returns the number of rows that were deleted from the database.
        :rtype: JSON String
        """

        getAll = "SELECT * FROM AIS_MESSAGE WHERE ais_timestamp < DATE_SUB(NOW(), INTERVAL 5 MINUTE);"
        results = connector.run(con, getAll)

        sqlA = """DELETE FROM ais_message WHERE ais_timestamp < DATE_SUB(NOW(), INTERVAL 5 MINUTE);"""
        connector.run(con, sqlA)

        getRemaining = "SELECT * FROM AIS_MESSAGE"
        remaining = connector.run(con, getRemaining)

        return importedJson.dumps(len(results) - len(remaining))

    def readAllRecentPositions(con):
        """Returns all most recent ships positions.
        :param con: A connection object.
        :type con: mysq.connector.connection.MysqlConnection
        :return: Returns data pertaining to the ships as well as their position (latitude and longitude)
        :rtype: Array
        """
        query = """
        SELECT VESSEL.IMO, VESSEL.NAME, VESSEL.MMSI, POSITION_REPORT.Latitude, POSITION_REPORT.Longitude
        FROM VESSEL, AIS_MESSAGE, POSITION_REPORT
        WHERE AIS_Timestamp = (
            SELECT MAX(AIS_MESSAGE.AIS_Timestamp)
            FROM VESSEL, AIS_MESSAGE
            WHERE VESSEL.MMSI = AIS_MESSAGE.MMSI
        )
        AND POSITION_REPORT.Id = AIS_MESSAGE.ID
        AND VESSEL.MMSI = AIS_MESSAGE.MMSI;
        """

        finalArray = []
        results =  connector.run(con, query)
        for result in results:
            shipData = {"MMSI": result[2], "lat": float(result[3]), "long": float(result[4]), "IMO": result[0], "Name": result[1]}
            finalArray.append(shipData)
        return finalArray

    def readOneRecentPosition(con, mmsi):
        """Returns the most recent ship position given the ship's mmsi.
        :param con: A connection object.
        :type con: mysq.connector.connection.MysqlConnection
        :param mmsi: A ship/vessel mmsi.
        :type mmsi: int
        :return: Returns data pertaining to the ships as well as their position (latitude and longitude)
        :rtype: JSON String
        """
        if type(mmsi) != int:
            return importedJson.dumps({})
        
        query = "SELECT VESSEL.IMO, VESSEL.NAME, VESSEL.MMSI, POSITION_REPORT.Latitude, POSITION_REPORT.Longitude FROM VESSEL, AIS_MESSAGE, POSITION_REPORT WHERE AIS_Timestamp = (SELECT MAX(AIS_MESSAGE.AIS_Timestamp) FROM VESSEL, AIS_MESSAGE WHERE VESSEL.MMSI = AIS_MESSAGE.MMSI) AND POSITION_REPORT.Id = AIS_MESSAGE.ID AND VESSEL.MMSI = AIS_MESSAGE.MMSI AND AIS_MESSAGE.MMSI = {};".format(mmsi)
        result = connector.run(con, query)
        resultjson = {"MMSI": result[0][2], "lat": float(result[0][3]), "long": float(result[0][4]), "IMO": result[0][0], "Name": result[0][1]}
        return importedJson.dumps(resultjson)

    def readShipPositionToPort(con, portName, country):
        """Returns ship positions in tile of scale 3 containing the given port.
        :param con: A connection object.
        :type con: mysq.connector.connection.MysqlConnection
        :param portName: The name of the port to be searched.
        :type portName: String
        :return: Returns either an array of ship position documents, or an array of port documents.
        :rtype: Array
        """
        if type(portName) != str or type(country) != str:
            return []

        getPorts = "SELECT * FROM PORT, MAP_VIEW WHERE PORT.Port_Location = '{}' AND PORT.Country = '{}' AND PORT.MapView_3_Id = MAP_VIEW.Id;".format(portName, country)
        results = connector.run(con, getPorts)
        if len(results) > 1:
            finalArray = []
            for result in results:
                formatted = {"Id": result[0],"Name": result[2],"Country": result[3],"lat": float(result[5]),"long": float(result[4]), "scale_1": result[7], "scale_2": result[8],"scale_3": result[9]}
                finalArray.append(formatted)
            return finalArray
        elif len(results) == 1:
            getShips = "SELECT VESSEL.MMSI, POSITION_REPORT.Latitude, POSITION_REPORT.Longitude, VESSEL.IMO FROM VESSEL, AIS_MESSAGE, POSITION_REPORT, MAP_VIEW, PORT WHERE VESSEL.MMSI = AIS_MESSAGE.MMSI AND AIS_MESSAGE.Id = POSITION_REPORT.Id AND PORT.Port_Location = '{}' AND PORT.Country = '{}' AND PORT.MapView_3_Id = MAP_VIEW.Id AND MAP_VIEW.ImageSouth < POSITION_REPORT.Latitude < MAP_VIEW.ImageNorth AND MAP_VIEW.ImageWest < POSITION_REPORT.Longitude < MAP_VIEW.ImageEast;".format(portName, country)
            finalShipArray = []
            shipresults = connector.run(con, getShips)
            for shipresult in shipresults:
                reformatted = {"MMSI": shipresult[0], "lat": float(shipresult[1]), "long": float(shipresult[2]), "IMO": shipresult[3]}
                finalShipArray.append(reformatted)
            return finalShipArray
        else:
            return []

    def getPortByName(con,port_location,country):
        '''
        Read all ports matching the given name and (optional) country
        :param con: A connection object.
        :type con: mysq.connector.connection.MysqlConnection
        :param port_location: the name of a port
        :type port_location: string
        :param country: (optional) the country a port is in
        :type country: string
        :return: retuns an array of port documents
        :rtype: array of dictionaries
        '''
        
        port_documents = []

        if type(port_location) != type("hi"):
            return port_documents
        
        
        if country == None:
               
            query = "SELECT PORT.Id, PORT.Port_Location, PORT.Country,  PORT.Latitude, PORT.Longitude, PORT.MapView_1_Id, PORT.MapView_2_Id, PORT.MapView_3_Id FROM PORT, MAP_VIEW AS MAP1, MAP_VIEW AS MAP2, MAP_VIEW AS MAP3 WHERE PORT.Port_Location = '{}' AND PORT.MapView_1_Id = MAP1.Id AND PORT.MapView_2_Id = MAP2.Id AND PORT.MapView_3_Id = MAP3.Id;".format(port_location)
        
        
        elif country != None:

            if type(country) != type("hi"):
                return port_documents
        
               
            query = "SELECT PORT.Id, PORT.Port_Location, PORT.Country,  PORT.Latitude, PORT.Longitude, PORT.MapView_1_Id, PORT.MapView_2_Id, PORT.MapView_3_Id FROM PORT, MAP_VIEW AS MAP1, MAP_VIEW AS MAP2, MAP_VIEW AS MAP3 WHERE PORT.Port_Location = '{}' AND PORT.Country = '{}' AND PORT.MapView_1_Id = MAP1.Id AND PORT.MapView_2_Id = MAP2.Id AND PORT.MapView_3_Id = MAP3.Id;".format(port_location,country)
        
        result = connector.run(con,query)
        
        for item in result:
            port_documents.append({"Id":item[0],"Name":item[1],"Country":item[2],"Latitude":float(item[3]),"Longitude":float(item[4]),"Map View 1":item[5],"Map View 2":item[6],"Map View 3":item[7] })

        return port_documents
        
    
    def ShipPositionsInTile(con, Tile_Id):
        '''
        Read all most recent ship positions in a specified tile 
        :param con: A connection object.
        :type con: mysq.connector.connection.MysqlConnection
        :param Tile_Id: the id of a map tile
        :type Tile_Id: integer
        :return: retuns an array of ship documents
        :rtype: array of dictionaries
        '''
        
        if( type(Tile_Id) != int ):
            return []

        vessel_documents = []
        query = '''SELECT VESSEL.MMSI, POSITION_REPORT.Latitude, POSITION_REPORT.Longitude, VESSEL.IMO, VESSEL.Name  
        FROM VESSEL, AIS_MESSAGE, POSITION_REPORT, MAP_VIEW 
        WHERE AIS_MESSAGE.AIS_Timestamp = (SELECT MAX(AIS_MESSAGE.AIS_Timestamp) FROM VESSEL, AIS_MESSAGE, POSITION_REPORT WHERE VESSEL.MMSI = AIS_MESSAGE.MMSI)
        AND VESSEL.MMSI = AIS_MESSAGE.MMSI 
        AND POSITION_REPORT.Id = AIS_MESSAGE.Id 
        AND MAP_VIEW.ID = '{}' 
        AND MAP_VIEW.ImageWest < POSITION_REPORT.Longitude < MAP_VIEW.ImageEast 
        AND MAP_VIEW.ImageSouth < POSITION_REPORT.Latitude < MAP_VIEW.ImageNorth;'''.format(Tile_Id)
        
        results = connector.run(con, query)
        for item in results:
            vessel_documents.append({"MMSI":item[0],"Latitude":float(item[1]),"Longitude":float(item[2]),"IMO":item[3],"Name":item[4]})

        return vessel_documents


    def readLastFivePositions(con, mmsi):
        """Returns the last 5 positions of the ship given the mmsi.
        :param con: A connection object.
        :type con: mysq.connector.connection.MysqlConnection
        :param mmsi: A ship/vessel mmsi.
        :type mmsi: int
        :return: Returns the last 5 positions of the ship with the given mmsi.
        :rtype: JSON String
        """
        if type(mmsi) != int:
            return importedJson.dumps({})
        
        query = "SELECT VESSEL.MMSI, POSITION_REPORT.Latitude, POSITION_REPORT.Longitude, VESSEL.IMO FROM VESSEL, AIS_MESSAGE, POSITION_REPORT WHERE VESSEL.MMSI = AIS_MESSAGE.MMSI AND AIS_MESSAGE.Id = POSITION_REPORT.Id AND AIS_MESSAGE.MMSI = '{}' ORDER BY AIS_MESSAGE.AIS_Timestamp DESC LIMIT 5;".format(mmsi)
        results = connector.run(con, query)

        finalResult = {"MMSI": results[0][0]}
        finalArray = []
        for result in results:
            position = {"lat": float(result[1]),"long": float(result[2])}
            finalArray.append(position)
        finalResult["Positions"] = finalArray
        finalResult["IMO"] = results[0][3]
        return importedJson.dumps(finalResult)

    def readVesselInformationByCriteria(con, mmsi, imo, name, callsing):
        '''
        Reads vessel information given certain criteria
        :param con: A connection object
        :type con: mysq.connector.connection.MysqlConnection
        :param mmsi: The mmsi from a ship
        :type mmsi: integer
        :param imo: The imo from a ship
        :type imo: integer
        :param name: The name from a ship
        :type name: string
        :param callsign: The callsign from a ship
        :type callsign: string
        :returns: Returns relevant information about a vessel
        :rtype: JSON string
        '''


        if type(mmsi) != int or (imo != None and type(imo) != int) or (name != None and type(name) != str) or (callsing != None and type(callsing) != str):
            return importedJson.dumps({})
        else:
            sqlQueryPermanent = "SELECT * FROM VESSEL WHERE MMSI = {} ".format(mmsi)
            sqlQueryTransient = "SELECT * FROM static_data WHERE Id IN (SELECT Id FROM AIS_MESSAGE WHERE MMSI = {}) ".format(mmsi)
            if imo != None:
                sqlQueryPermanent += "AND IMO = {} ".format(imo)
                sqlQueryTransient += "AND IMO = {} ".format(imo)
            if name != None:
                sqlQueryPermanent += "AND Name = '{}' ".format(name)
                sqlQueryTransient += "AND Name = '{}' ".format(name)
            if callsing != None:
                sqlQueryPermanent += "AND CallSign = '{}' ".format(callsing)
                sqlQueryTransient += "AND CallSign = '{}' ".format(callsing)
            sqlQueryPermanent += ";"
            sqlQueryTransient += ";"
            results = connector.run(con, sqlQueryPermanent)
            results2 = connector.run(con, sqlQueryTransient)
            resultsjson = {"IMO": results[0][0], "Flag": results[0][1], "Name": results[0][2], "Built": results[0][3], "Callsign": results[0][4], "Lenght": results[0][5], "Breadth": results[0][6], "Tonage": results[0][7], "MMSI": results[0][8], "Type": results[0][9], "Status": results[0][10], "Order": results[0][11]}
            if len(results2) != 0:
                results2json = {"IMO": results2[0][1], "CallSign": results2[0][2], "Name": results2[0][3], "Vessel_Type": results2[0][4], "Cargo_Type": results2[0][5], "Length": results2[0][6], "Breadth": results2[0][7], "Draught":float(results2[0][8]), "AIS_Destination": results2[0][9], "ETA": str(results2[0][10]), "A": results2[0][11], "B": results2[0][12], "C": results2[0][13], "D": results2[0][14]}
                for a in results2json:
                    if a not in resultsjson:
                        resultsjson[a] = results2json[a]
        
            return importedJson.dumps(resultsjson)

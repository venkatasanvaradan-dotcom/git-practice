create database supplychain;
use supplychain;


DELIMITER $$
CREATE  PROCEDURE `Equipment_Category`()
BEGIN
	DROP TABLE IF EXISTS EquipmentCategory;
	CREATE TABLE EquipmentCategory (
	Equipment_Alfa2Code varchar(50) NOT NULL,
	Equipment_Type varchar(255));
	INSERT INTO EquipmentCategory(Equipment_Alfa2Code,Equipment_Type)
		SELECT distinct SUBSTRING(`Primary Machine Name`,1,2),`Primary Machine Category Name` From cycle
        group by SUBSTRING(`Primary Machine Name`,1,2);
        
	INSERT INTO EquipmentCategory(Equipment_Alfa2Code,Equipment_Type)
		SELECT distinct SUBSTRING(`Secondary Machine Name`,1,2),`Secondary Machine Category Name` From cycle
        WHERE SUBSTRING(`Secondary Machine Name`,1,2) not in (select Equipment_Alfa2Code from EquipmentCategory)
        group by SUBSTRING(`Secondary Machine Name`,1,2);
		select * from EquipmentCategory;
END$$
DELIMITER ;
call `Equipment_Category`();

DELIMITER $$

CREATE  PROCEDURE `Equipment_Type_Master`()
BEGIN
	DROP TABLE IF EXISTS EquipmentTypeMaster;
	CREATE TABLE EquipmentTypeMaster (
	Type_Id int AUTO_INCREMENT PRIMARY KEY,
	Type_Code varchar(255) NOT NULL,
	Equipment_Type varchar(255));
	INSERT INTO EquipmentTypeMaster(Type_Code,Equipment_Type)
		SELECT distinct Equipment_Alfa2Code  ,Equipment_Type From EquipmentCategory;
        select * from EquipmentTypeMaster;
END$$
DELIMITER ;
call `Equipment_Type_Master`();

DELIMITER $$
CREATE PROCEDURE `Equipment_Master`()
BEGIN
DROP TABLE IF EXISTS Equipment_Master;
CREATE TABLE Equipment_Master (
Equipment_Id int AUTO_INCREMENT PRIMARY KEY,
Name varchar(255) NOT NULL,
Type_Id int,
Class varchar(255),
Equipment_Capacity varchar(255),
FOREIGN KEY (Type_Id) REFERENCES equipmenttypemaster(Type_Id));

INSERT Into Equipment_Master (Name, Type_Id, Class, Equipment_Capacity)
	select DISTINCT(t1.`Primary Machine Name`) as Name, t2.Type_Id as Type_Id,
	t1.`Primary Machine Class Name` as Class, t1.`Payload (t)` as Equipment_Capacity
	from cycle as t1
	join equipmenttypemaster as t2 on SUBSTRING(`Primary Machine Name`,1,2) = Type_Code;
    
Insert Into Equipment_Master (Name, Type_Id, Class, Equipment_Capacity)
	select DISTINCT t1.`Secondary Machine Name` as Name, t2.Type_Id as Type_Id,
	t1.`Secondary Machine Class Name` as Class, t1.`Payload (t)` as Equipment_Capacity
	from cycle as t1
	join equipmenttypemaster as t2 on SUBSTRING(`Secondary Machine Name`,1,2) = Type_Code;
END$$
DELIMITER ;
call `Equipment_Master`();

DELIMITER $$
CREATE  PROCEDURE `MovementData`()
BEGIN
DROP TABLE IF EXISTS Movement_Data;
CREATE TABLE Movement_Data
	(SELECT `Primary Machine Name` as Machine_Name,
		`Source Location Name` as Source_Location,
		`Destination Location Name` as Destination_Location,
		`Payload (kg)` as Payload,
		`Cycle Start Timestamp (GMT8)` as Start_Time,
		`Cycle End Timestamp (GMT8)` as End_Time
	FROM cycle);

	ALTER TABLE Movement_Data ADD Movement_Id int auto_increment primary key not null ;
	
END$$
DELIMITER ;
call `MovementData`();


DELIMITER $$
CREATE PROCEDURE `DelayData`()
BEGIN
DROP TABLE IF EXISTS Delay_Data;
CREATE TABLE Delay_Data
	SELECT `Delay OID`,
		`Description`,
		`Delay Class Name`,
		`Delay Class Category Name`,
		`Target Machine Name` as Machine_Name,
		`Delay Start Timestamp (GMT8)` as Delay_Start_Timestamp,
		`Delay Finish Timestamp (GMT8)` as Delay_Finish_Timestamp,
		TIMESTAMPDIFF(second, `Delay Start Timestamp (GMT8)`, `Delay Finish Timestamp (GMT8)`) as Delay_Time
	FROM delay;
	select * from Delay_Data;
END$$
DELIMITER ;
call `DelayData`();


DELIMITER $$
CREATE  PROCEDURE `CycleData`()
BEGIN
DROP TABLE IF EXISTS Cycle_Data;
CREATE TABLE Cycle_Data
	SELECT md.Movement_Id as Movement_Id, 
		md.Machine_Name,
		cd.`Cycle Type` as Cycle_Type,
		cd.`Source Location Name` as 'Source Location', 
		cd.`Destination Location Name` as 'Destination Location',
		cd.`Cycle Start Timestamp (GMT8)` as 'CYCLE START TIME', 
		cd.`Cycle End Timestamp (GMT8)` as 'CYCLE END TIME',
		cd.TRUCKQUEUEATSOURCEDURATION as 'Source Wait Time',
		cd.QUEUEATSINKDURATION as 'Destination Wait Time',
		TIMESTAMPDIFF(second, cd.`Cycle Start Timestamp (GMT8)`, cd.`Cycle End Timestamp (GMT8)`) as Cycle_Time
	FROM cycle cd, Movement_Data md
	WHERE cd.`Primary Machine Name` = md.Machine_Name AND
		cd.`Source Location Name` = md.Source_Location AND
		cd.`Cycle Start Timestamp (GMT8)` = md.Start_Time
		ORDER BY Movement_Id;

	ALTER TABLE Cycle_Data ADD Cycle_Id int auto_increment primary key not null;
	ALTER TABLE Cycle_Data ADD `Source Location Wait` varchar(255);
	ALTER TABLE Cycle_Data ADD `Destination Location Wait` varchar(255);

	UPDATE Cycle_Data SET `Source Location Wait` = 'N' WHERE (`Source Wait Time` is NULL and Cycle_Id <> 0);
	UPDATE Cycle_Data SET `Source Location Wait` = 'Y' WHERE `Source Wait Time` is not null and Cycle_Id <> 0;

	UPDATE Cycle_Data SET `Destination Location Wait` = 'N' WHERE `Destination Wait Time` is NULL and Cycle_Id <> 0;
	UPDATE Cycle_Data SET `Destination Location Wait` = 'Y' WHERE `Destination Wait Time` is not NULL and Cycle_Id <> 0;

	ALTER TABLE  Cycle_Data ADD Avg_Rate float;
	ALTER TABLE  Cycle_Data ADD Max_Rate float;
	
	UPDATE Cycle_Data cd INNER JOIN
		(SELECT `Primary Machine Name`, 
		`Cycle Type`, 
		SUM(`Payload (t)`) / (SUM(`Cycle Duration`)/3600) AS calcAvg
		FROM cycle
		WHERE  (`Cycle Type` = 'LoaderCycle' ) 
		GROUP BY `Primary Machine Name`, `Cycle Type`) AS tmpT 
	ON tmpT.`Primary Machine Name` = cd.Machine_Name 
    SET Avg_Rate = calcAvg where cd.Cycle_Id <> 0;
	

	
	UPDATE Cycle_Data pro_cd INNER JOIN 
		(SELECT Machine_Name, MAX(Avg_Rate) Avg_Rate 
		FROM Cycle_Data 
		GROUP BY Machine_Name) as temp2
	ON pro_cd.Machine_Name = temp2.Machine_Name
    SET Max_Rate = temp2.Avg_Rate where Cycle_Id <> 0;

END$$
DELIMITER ;
call `CycleData`();

DELIMITER $$
CREATE  PROCEDURE `OEE`()
BEGIN
	DROP TABLE IF EXISTS Run_Time;
    CREATE TABLE Run_Time
	SELECT `Primary Machine Name` as Machine, 
		SUM(`COMPLETEDCYCLEDURATION`) as Total_Run_Time,
		Min(`COMPLETEDCYCLEDURATION`) as Min_Run_Time,
		COUNT(`COMPLETEDCYCLEDURATION`) as Run_Time_Count,
		CAST(`Cycle Start Timestamp (GMT8)` as Date) as Start_Date
	FROM cycle
	WHERE `Cycle Type` = 'TruckCycle' AND COMPLETEDCYCLEDURATION > 0
	GROUP BY `Primary Machine Name`, CAST(`Cycle Start Timestamp (GMT8)` as Date);

	DROP TABLE IF EXISTS Good_Count;
    CREATE TABLE Good_Count
	SELECT `Primary Machine Name` Machine, SUM(`COMPLETEDCYCLEDURATION`) Good_Count, 
    CAST(`Cycle Start Timestamp (GMT8)` as DATE) as Start_Date
	FROM cycle
	WHERE ((`Primary Machine Name` LIKE 'DT%' AND `Secondary Machine Name` LIKE 'EX%') OR (`Primary Machine Name` LIKE 'EX%' AND `Secondary Machine Name` LIKE 'DT%')) 
		AND (`Source Location Description` LIKE 'CR%' OR `Destination Location Description` LIKE 'CR%'
			OR `Source Location Name` LIKE '%FILL%' OR `Destination Location Name` LIKE '%FILL%'
			OR `Source Location Name` LIKE '%WASTE%' OR `Destination Location Name` LIKE '%WASTE%')
	GROUP BY `Primary Machine Name`, CAST(`Cycle Start Timestamp (GMT8)` as DATE);
	
    DROP TABLE IF EXISTS Total_Count;
    CREATE TABLE Total_Count
	SELECT `Primary Machine Name` Machine, SUM(`COMPLETEDCYCLEDURATION`) Total_Count, CAST(`Cycle Start Timestamp (GMT8)` as DATE) as Start_Date
	FROM cycle
	GROUP BY `Primary Machine Name`, CAST(`Cycle Start Timestamp (GMT8)` as DATE);

	DROP TABLE IF EXISTS Quality;
    CREATE TABLE Quality
	SELECT t1.Machine, ROUND((t1.Good_Count/t2.Total_Count),2) as Quality, t1.Start_Date
	FROM Good_Count as t1, Total_Count as t2
	WHERE t1.Machine = t2.Machine AND t1.Start_Date = t2.Start_Date AND t2.Total_Count <> 0;
	
    DROP TABLE IF EXISTS OEE;
    CREATE TABLE OEE
	SELECT Machine, Availablity, Performance, Quality, 
	(Availablity* Performance* Quality) as OEE,
	Start_Date
	FROM
	(SELECT t1.Machine, 
		ROUND(Total_Run_Time/(24*60*60), 1) Availablity, 
		ROUND(((MIN_Run_Time*Run_Time_Count)/Total_Run_Time),1) AS Performance,
		t2.Quality Quality,
		t1.Start_Date
	FROM Run_Time as t1, Quality as t2
	WHERE t1.Machine = t2.Machine AND t1.Start_Date = t2.Start_Date) as t1;

END$$
DELIMITER ;
call `OEE`();

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `Location_Master`()
BEGIN

CREATE TABLE Location_Master (
Location_Id int auto_increment PRIMARY KEY,
Name varchar(255),
Latitude varchar(255) ,
Longitude varchar(255));

INSERT INTO Location_Master(Name, Latitude, Longitude)
SELECT Name, avg(Latitude) as Latitude, avg(Longitude) as Longitude 
FROM location where Name <> ''
group by Name;

END$$
DELIMITER ;
call `Location_Master`();


DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `Location_Type_Master`()
BEGIN

CREATE TABLE Location_Type_Master (
Type_Id int auto_increment PRIMARY KEY,
Type_Name varchar(255) NOT NULL);
END$$
DELIMITER ;

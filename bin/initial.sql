/*
SQLyog Ultimate v11.11 (32 bit)
MySQL - 5.0.77 : Database - ocsearch_1.0
*********************************************************************
*/


/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

CREATE DATABASE /*!32312 IF NOT EXISTS*/`ocsearch` /*!40100 DEFAULT CHARACTER SET utf8 */;

CREATE USER ocsearch@'%' IDENTIFIED BY 'ocsearch';
grant all privileges on ocsearch.* to ocsearch@'%' IDENTIFIED BY 'ocsearch';
USE `ocsearch`;

/*Table structure for table `table_def` */

DROP TABLE IF EXISTS `schema_def`;
CREATE TABLE `schema_def` (
  `name` varchar(255) NOT NULL,
  `rowkey_expression` varchar(255) NOT NULL,
  `table_expression` varchar(255) NOT NULL,
  `content_field` varchar(255) NOT NULL, /*json 字符串*/
  `query_fields` varchar(255) NOT NULL, /*json 字符串*/
  PRIMARY KEY  (`name`)

) ENGINE=InnoDB DEFAULT CHARSET=utf8 CHECKSUM=1 DELAY_KEY_WRITE=1 ROW_FORMAT=DYNAMIC;

/*Data for the table `schema_def` */

LOCK TABLES `schema_def` WRITE;

UNLOCK TABLES;


/*Table structure for table `field_def` */

DROP TABLE IF EXISTS `field_def`;

CREATE TABLE `field_def` (
  `name` varchar(255) NOT NULL,
  `indexed` varchar(5) NOT NULL,
  `index_contented` varchar(5) NOT NULL,
  `index_stored` varchar(5) NOT NULL,
  `index_type` varchar(255) NOT NULL,
  `hbase_column` varchar(255) NOT NULL,
  `hbase_family` varchar(255) NOT NULL,
  `store_type` varchar(255) NOT NULL,
  `schema_name` varchar(255) NOT NULL,
  UNIQUE KEY `ak_key_2_schemas` (`name`,`schema_name`),
  KEY `field_fk_schema` (`schema_name`),
  CONSTRAINT `field_fk_schema` FOREIGN KEY (`schema_name`) REFERENCES `schema_def` (`name`) ON DELETE NO ACTION ON UPDATE NO ACTION

) ENGINE=InnoDB DEFAULT CHARSET=utf8 CHECKSUM=1 DELAY_KEY_WRITE=1 ROW_FORMAT=DYNAMIC;

/*Data for the table `field_def` */

LOCK TABLES `field_def` WRITE;

UNLOCK TABLES;


/*Table structure for table `table_def` */

DROP TABLE IF EXISTS `table_def`;

CREATE TABLE `table_def` (
  `name` varchar(255) NOT NULL,
  `index_type` INT(1) NOT NULL,
  `hbase_regions` INT(4) NOT NULL,
  `solr_shards` INT(4) NOT NULL,
  `solr_replicas` INT(4) NOT NULL,
  `region_splits` varchar(255) NOT NULL,
  `schema_name` varchar(255) NOT NULL,
  KEY `table_fk_schema` (`schema_name`),
  PRIMARY KEY  (`name`),
  CONSTRAINT `table_fk_schema` FOREIGN KEY (`schema_name`) REFERENCES `schema_def` (`name`) ON DELETE NO ACTION ON UPDATE NO ACTION

) ENGINE=InnoDB DEFAULT CHARSET=utf8 CHECKSUM=1 DELAY_KEY_WRITE=1 ROW_FORMAT=DYNAMIC;

/*Data for the table `table_def` */

LOCK TABLES `table_def` WRITE;

UNLOCK TABLES;


/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
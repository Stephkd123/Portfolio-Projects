-- CREATE SCHEMA stage ;
-- GO

CREATE TABLE stage.DIM_DATE (
date_key  INT  NOT NULL, 
date_value DATE       NOT NULL,	
date_short	CHAR(12)  NOT NULL,
date_medium  CHAR(16) NOT NULL,
date_long	CHAR(24)  NOT NULL,
date_full   CHAR(32)  NOT NULL, 
day_in_year	 SMALLINT   NOT NULL, 
day_in_month	SMALLINT  NOT NULL, 
is_first_day_in_montH	CHAR(10) NOT NULL,
is_last_day_in_month	CHAR(10) NOT NULL, 
day_abbreviation		CHAR(3) NOT NULL, 
day_name	CHAR(12) NOT NULL, 
week_in_year SMALLINT NOT NULL,
week_in_month	SMALLINT 	NOT NULL,
is_first_day_in_week CHAR(10) NOT NULL,
is_last_day_in_week	CHAR(10) NOT NULL,
month_number		SMALLINT NOT NULL,
month_abbreviation	CHAR(3) NOT NULL,
month_name		CHAR(12) NOT NULL,
year2			CHAR(2) NOT NULL,
year4			SMALLINT NOT NULL,
quarter_name		CHAR(2) NOT NULL,
quarter_number		SMALLINT NOT NULL,
year_quarter		CHAR(7) NOT NULL,
year_month_number	CHAR(7) NOT NULL,
year_month_abbreviation	CHAR(8) NOT NULL	
)
GO

DROP TABLE IF EXISTS stage.DIM_DATE;
GO 
CREATE TABLE stage.DIM_DATE (
date_key  INT  NOT NULL, 
date_value DATE       NOT NULL,	
date_short	CHAR(12)  NOT NULL,
date_medium  CHAR(16) NOT NULL,
date_long	CHAR(24)  NOT NULL,
date_full   CHAR(32)  NOT NULL, 
day_in_year	 SMALLINT   NOT NULL, 
day_in_month	SMALLINT  NOT NULL, 
is_first_day_in_montH	CHAR(10) NOT NULL,
is_last_day_in_month	CHAR(10) NOT NULL, 
day_abbreviation		CHAR(3) NOT NULL, 
day_name	CHAR(12) NOT NULL, 
week_in_year SMALLINT NOT NULL,
week_in_month	SMALLINT 	NOT NULL,
is_first_day_in_week CHAR(10) NOT NULL,
is_last_day_in_week	CHAR(10) NOT NULL,
month_number		SMALLINT NOT NULL,
month_abbreviation	CHAR(3) NOT NULL,
month_name		CHAR(12) NOT NULL,
year2			CHAR(2) NOT NULL,
year4			SMALLINT NOT NULL,
quarter_name		CHAR(2) NOT NULL,
quarter_number		SMALLINT NOT NULL,
year_quarter		CHAR(7) NOT NULL,
year_month_number	CHAR(7) NOT NULL,
year_month_abbreviation	CHAR(8) NOT NULL	
)
GO

CREATE TABLE stage.DIM_TIME(
time_key   INT NOT NULL, 
time_value	TIME NOT NULL,
hours24	 SMALLINT NOT NULL,
hours12	 SMALLINT,
minutes	 SMALLINT,
seconds	 SMALLINT,
am_pm	 CHAR(3)
)
GO

DROP TABLE IF EXISTS SAKILA_DWH.DIM_STAFF;
GO 
CREATE TABLE stage.DIM_STAFF(
staff_key TINYINT IDENTITY(1,1) PRIMARY KEY,
staff_last_update DATETIME2 (0) DEFAULT '1970-01-01 00:00:00',
staff_first_name  VARCHAR(45),
staff_last_name	  VARCHAR(45),
staff_id	INT,
staff_store_id	INT,
staff_version_number  SMALLINT,
staff_valid_from	DATETIME,	
staff_valid_through DATETIME,	
staff_active CHAR(3)
)

DROP TABLE IF EXISTS stage.DIM_CUSTOMER;
GO
CREATE TABLE stage.DIM_CUSTOMER(
customer_key	INT  IDENTITY(1,1) NOT NULL,
customer_last_update  DATETIME2(0)DEFAULT '1970-01-01 00:00:00',	
customer_id				INT,
customer_first_name			VARCHAR(45),
customer_last_name			VARCHAR(45),
customer_email				VARCHAR(50),
customer_active				CHAR(3),
customer_created			DATE,	
customer_address			VARCHAR(64),
customer_district				VARCHAR(20),
customer_postal_code			VARCHAR(10),
customer_phone_number			VARCHAR(20),
customer_city				VARCHAR(50),
customer_country			VARCHAR(50),
customer_version_number		SMALLINT,
customer_valid_from			DATETIME,	
customer_valid_through			DATETIME

)
GO

DROP TABLE IF EXISTS stage.DIM_STORE;
GO 
CREATE TABLE stage.DIM_STORE (
store_key INT IDENTITY(1,1) NOT NULL,
store_last_update	DATETIME2(0) DEFAULT '1970-01-01 00:00:00',
store_id					INT,
store_address				VARCHAR(64),
store_district				VARCHAR(20),
store_postal_code			VARCHAR(10),
store_phone_number			VARCHAR(20),
store_city				VARCHAR(50),
store_country				VARCHAR(50),
store_manager_staff_id			INT,
store_manager_first_name		VARCHAR(45),
store_manager_last_name		VARCHAR(45),
store_version_number			SMALLINT,
store_valid_from				DATETIME,	
store_valid_through			DATETIME

)
GO

-- EXEC sp_columns 'DIM_STORE';
DROP TABLE IF EXISTS stage.DIM_FILM;
GO 


CREATE TABLE stage.DIM_FILM(
film_key	INT IDENTITY(1,1) NOT NULL,
film_last_update  DATETIME2 (0) DEFAULT '1970-01-01 00:00:00',
film_title	VARCHAR(64) NOT NULL,
film_description	VARCHAR (MAX) NOT NULL,
film_release_year	SMALLINT NOT NULL,
film_language  VARCHAR(20) NOT NULL,
film_original_language	VARCHAR(20) NOT NULL,
film_rental_duration	SMALLINT,
film_rental_rate				DECIMAL(4),
film_duration				INT,
film_replacement_cost			DECIMAL(5),
film_rating_code				CHAR(5),
film_rating_text				VARCHAR(30),
film_has_trailers				CHAR(4),
film_has_commentaries			CHAR(4),
film_has_deleted_scenes			CHAR(4),
film_has_behind_the_scenes		CHAR(4),
film_in_category_action			CHAR(4),
film_in_category_animation		CHAR(4),
film_in_category_children			CHAR(4),
film_in_category_classics			CHAR(4),
film_in_category_comedy			CHAR(4),
film_in_category_documentary		CHAR(4),
film_in_category_drama			CHAR(4),
film_in_category_family			CHAR(4),
film_in_category_foreign			CHAR(4),
film_in_category_games			CHAR(4),
film_in_category_horror			CHAR(4),
film_in_category_music			CHAR(4),
film_in_category_new			CHAR(4),
film_in_category_scifi			CHAR(4),
film_in_category_sports			CHAR(4),
film_in_category_travel			CHAR(4),
film_id		INT NOT NULL

)
DROP TABLE IF EXISTS stage.DIM_ACTOR;
GO 
CREATE TABLE stage.DIM_ACTOR(
actor_key	INT IDENTITY(1,1) NOT NULL,
actor_last_update	DATETIME2 (0) DEFAULT '1970-01-01 00:00:00',
actor_last_name		VARCHAR(45) NOT NULL,
actor_first_name	VARCHAR(45) NOT NULL,
actor_id		INT NOT NULL
)

DROP TABLE IF EXISTS stage.DIM_FILM_ACTOR_BRIDGER;
GO 
CREATE TABLE stage.DIM_FILM_ACTOR_BRIDGER(
film_key	INT IDENTITY(1,1) NOT NULL,
actor_key	INT NOT NULL,
actor_weighting_factor	DECIMAL(3) NOT NULL
)

DROP TABLE IF EXISTS stage.DIM_FILM_ACTOR_BRIDGER;
GO 

CREATE TABLE stage.FACT_RENTAL(
customer_key INT  IDENTITY(1,1) NOT NULL,
staff_key	TINYINT NOT NULL,
film_key	INT NOT NULL,
store_key	INT NOT NULL,
rental_date_key	INT NOT NULL,
return_date_key	INT NOT NULL,
rental_time_key	INT NOT NULL,
count_returns	INT NOT NULL,
count_rentals	INT NOT NULL,
rental_duration	INT,
rental_last_update  DATETIME2,
rental_id	INT	

)
GO



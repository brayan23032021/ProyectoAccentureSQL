CREATE DATABASE ACCENTURE
GO

USE ACCENTURE;

	CREATE TABLE BaseAtenciones(
		num_ticket nvarchar(20) NULL,
		fecha_programada nvarchar(10) NULL,
		fecha_creacion nvarchar(10) NULL,
		fecha_cierre nvarchar(10) NULL,
		estado nvarchar(50) NULL,
		agencia nvarchar(100) NULL,
		service_desk nvarchar(100) NULL,
		tipo_ticket nvarchar(50) NULL,
		proveedor nvarchar(200) NULL,
		costo nvarchar(50) NULL,
		numero_OS nvarchar(50) NULL
	);


CREATE TABLE BaseTickets(
	num_ticket nvarchar(20) NULL,
	categoria nvarchar(50) NULL,
	tipo nvarchar(50) NULL,
	detalle nvarchar(50) NULL,
	matri nvarchar(50) NULL,
	ubicacion nvarchar(100) NULL,
	region nvarchar(50) NULL,
	modo_reporte nvarchar(50) NULL,
	estado nvarchar(50) NULL,
	prioridad tinyint NULL,
	fecha_creacion nvarchar(10) NULL,
	fecha_termino nvarchar(10) NULL,
	fecha_cierre nvarchar(10) NULL
);

-----------------------------------------------
USE ACCENTURE;

-- CREAR TABLAS


CREATE TABLE DimCategoria(
	CategoriaID int NOT NULL,
	Categoria nvarchar(50) NULL,
	CONSTRAINT PK_Categoria PRIMARY KEY (CategoriaID)
)

CREATE TABLE DimTipo(
	TipoID int NOT NULL,
	Tipo nvarchar(50) NULL,
	CONSTRAINT PK_Tipo PRIMARY KEY (TipoID)
)


CREATE TABLE DimDetalle(
	DetalleID int NOT NULL,
	Detalle nvarchar(50) NULL,
	CONSTRAINT PK_Detalle PRIMARY KEY (DetalleID)
)


CREATE TABLE Atenciones(
	num_ticket nvarchar(20) NOT NULL,
	agenciaID nvarchar(100) NULL,
	categoriaID int NOT NULL,
	tipoID int NOT NULL,
	detalleID int NOT NULL,
	fecha_creacion date NULL,
	fecha_programada date NULL,
	fecha_real_fin date NULL,
	estado nvarchar(50) NULL,
	service_desk nvarchar(100) NULL,
	tipo_ticket nvarchar(50) NULL,
	proveedor nvarchar(200) NULL,
	costo money NULL,
	FOREIGN KEY (categoriaID) REFERENCES DimCategoria(CategoriaID),
	FOREIGN KEY (tipoID) REFERENCES DimTipo(TipoID),
	FOREIGN KEY (detalleID) REFERENCES DimDetalle(DetalleID)
)


---------------------------------------------------------
	TRUNCATE TABLE ACCENTURE.dbo.BaseAtenciones;
	TRUNCATE TABLE ACCENTURE.dbo.BaseTickets;
	TRUNCATE TABLE ACCENTURE.dbo.Atenciones;

---------------------------------------------------------


BULK INSERT BaseAtenciones
	FROM 'C:\Users\fabian vergara\Desktop\proyecto sql\Atenciones.csv'
	WITH (
			FORMAT = 'CSV'
			, FIRSTROW = 2
			, FIELDTERMINATOR = ';'
			, ROWTERMINATOR = '\n'
		)


	BULK INSERT BaseTickets
	FROM 'C:\Users\fabian vergara\Desktop\proyecto sql\Tickets.csv'
	WITH (
			FORMAT = 'CSV'
			, FIRSTROW = 2
			, FIELDTERMINATOR = ';'
			, ROWTERMINATOR = '\n'
		)

	BULK INSERT DimCategoria
	FROM 'C:\Users\fabian vergara\Desktop\proyecto sql\Categoria.csv'
	WITH (
			FORMAT = 'CSV'
			, FIRSTROW = 2
			, FIELDTERMINATOR = ';'
			, ROWTERMINATOR = '\n'
		)


	BULK INSERT DimTipo
	FROM 'C:\Users\fabian vergara\Desktop\proyecto sql\Tipo.csv'
	WITH (
			FORMAT = 'CSV'
			, FIRSTROW = 2
			, FIELDTERMINATOR = ';'
			, ROWTERMINATOR = '\n'
		)


	BULK INSERT DimDetalle
	FROM 'C:\Users\fabian vergara\Desktop\proyecto sql\Detalle.csv'
	WITH (
			FORMAT = 'CSV'
			, FIRSTROW = 2
			, FIELDTERMINATOR = ';'
			, ROWTERMINATOR = '\n'
		)


 SELECT * FROM Atenciones
 SELECT * FROM BaseAtenciones
 SELECT * FROM BaseTickets
 SELECT * FROM DimCategoria
 SELECT * FROM DimDetalle
 SELECT * FROM DimTipo


 ---------------------------------------------

 	SELECT 
		num_ticket
		, categoria
		, tipo
		, detalle
		, ubicacion
		,

		CASE estado
			WHEN 'Terminado' THEN 'Cerrado'
			ELSE estado
		END as estado,
	
		CONVERT(date, TRIM(fecha_creacion), 103) as 'fecha_creacion',
		CONVERT(date, TRIM(COALESCE(fecha_termino, fecha_cierre)), 103) as 'fecha_real_fin',

		CASE CHARINDEX(' - ', ubicacion)
		WHEN 0 THEN NULL
		ELSE CHARINDEX(' - ', ubicacion) + 3
		END as inicio,
		len(ubicacion) as fin
	
	INTO ETL_tickets1
	FROM BaseTickets


------------------------------------------------


	SELECT	A.num_ticket,
			
			CASE A.inicio
				WHEN NULL THEN NULL
				ELSE TRIM(SUBSTRING(A.ubicacion, A.inicio, A.fin))
			END as 'AgenciaID',
			
			CASE C.CategoriaID
				WHEN NULL THEN 10
				ELSE C.CategoriaID
			END as 'CategoriaID',
			
			CASE T.TipoID
				WHEN NULL THEN 100
				ELSE T.TipoID
			END as 'TipoID',

			CASE D.DetalleID
				WHEN NULL THEN 100
				ELSE D.DetalleID
			END as 'DetalleID',

			A.estado,
			A.fecha_creacion,
			A.fecha_real_fin
	INTO ETL_tickets2
	FROM ETL_tickets1 A
	LEFT JOIN ACCENTURE.dbo.DimCategoria C
	ON A.categoria = C.Categoria
	LEFT JOIN ACCENTURE.dbo.DimTipo T
	ON A.tipo = T.Tipo
	LEFT JOIN ACCENTURE.dbo.DimDetalle D
	ON A.detalle = D.Detalle;


--------------------------------------------------

	SELECT	num_ticket,

	CONVERT(date, TRIM(fecha_programada), 103) as 'fecha_programada',

	TRIM(service_desk) as 'service_desk',

	CASE LEFT(UPPER(TRIM(tipo_ticket)), 4)
		WHEN 'DIFE' THEN 'FLAT'
		WHEN 'VARI' THEN 'VARIABLE'
		ELSE UPPER(TRIM(tipo_ticket))
	END as 'tipo_ticket',

	TRIM(proveedor) as 'proveedor',

	CAST(
	CASE costo
		WHEN 'SIN COSTO' then NULL
		ELSE costo
	END 
	as money)
	as 'costo'

	INTO ETL_atenciones1
	FROM BaseAtenciones;

------------------------------------------------------
	
	INSERT INTO ACCENTURE.dbo.Atenciones
	SELECT	A.num_ticket,
			T.AgenciaID,
			T.CategoriaID,
			T.TipoID,
			T.DetalleID,
			T.fecha_creacion, 
			A.fecha_programada,
			T.fecha_real_fin,
			T.estado,
			A.service_desk,
			A.tipo_ticket,
			A.proveedor,
			A.costo
	FROM ETL_atenciones1 as A
	INNER JOIN ETL_tickets2 as T
	ON A.num_ticket = T.num_ticket;

	SELECT * FROM Atenciones
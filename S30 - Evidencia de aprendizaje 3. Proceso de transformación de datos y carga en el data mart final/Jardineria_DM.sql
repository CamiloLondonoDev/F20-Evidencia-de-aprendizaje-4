-- Crear base de datos para el Data Mart
CREATE DATABASE Jardineria_DM;
GO
USE Jardineria_DM;
GO

-- Dimensión Tiempo
CREATE TABLE dbo.DimTiempo (
  ClaveFecha     INT PRIMARY KEY,        -- YYYYMMDD
  FechaCompleta  DATE NOT NULL,
  Dia            TINYINT,
  Mes            TINYINT,
  NombreMes      VARCHAR(20),
  Trimestre      TINYINT,
  Año            SMALLINT,
  DiaSemana      TINYINT,
  EsFinDeSemana  BIT
);

-- Dimensión Cliente (SCD Tipo 2)
CREATE TABLE dbo.DimCliente (
  ClaveCliente       INT IDENTITY(1,1) PRIMARY KEY,
  ClaveClienteNatural INT NOT NULL,
  NombreCliente      VARCHAR(100),
  Ciudad             VARCHAR(50),
  Pais               VARCHAR(50),
  LimiteCredito      DECIMAL(15,2),
  VigenteDesde       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  VigenteHasta       DATETIME2 NULL,
  EsActual           BIT NOT NULL DEFAULT 1
);
CREATE INDEX IX_DimCliente_ClaveNatural ON dbo.DimCliente(ClaveClienteNatural, EsActual);

-- Dimensión Categoria
CREATE TABLE dbo.DimCategoria (
  ClaveCategoria INT IDENTITY(1,1) PRIMARY KEY,
  NombreCategoria VARCHAR(100) UNIQUE,
  Descripcion     VARCHAR(4000)
);

-- Dimensión Producto
CREATE TABLE dbo.DimProducto (
  ClaveProducto    INT IDENTITY(1,1) PRIMARY KEY,
  CodigoProducto   VARCHAR(15) UNIQUE,
  NombreProducto   VARCHAR(200),
  ClaveCategoria   INT NULL,
  CantidadEnStock  SMALLINT,
  Precio           DECIMAL(15,2)
);
CREATE INDEX IX_DimProducto_CodigoProducto ON dbo.DimProducto(CodigoProducto);

-- Tabla de Hechos: Ventas
CREATE TABLE dbo.HechosVentas (
  ClaveVenta     BIGINT IDENTITY(1,1) PRIMARY KEY,
  ClaveFecha     INT NOT NULL,
  ClaveCliente   INT NOT NULL,
  ClaveProducto  INT NOT NULL,
  ClaveCategoria INT NULL,
  CodigoPedido   INT NOT NULL,
  Cantidad       INT,
  PrecioUnidad   DECIMAL(15,2),
  NumeroLinea    SMALLINT NULL,
  MontoVenta     AS (Cantidad * PrecioUnidad) PERSISTED,
  InsertadoEn    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Tabla de control ETL
CREATE TABLE dbo.ETL_Control (
  IdEjecucion INT IDENTITY(1,1) PRIMARY KEY,
  FechaInicio DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  FechaFin DATETIME2 NULL,
  NombreProceso VARCHAR(100),
  FilasInsertadas INT NULL,
  Estado VARCHAR(20),          -- 'OK', 'ERROR'
  MensajeError NVARCHAR(4000),
  ArchivoOrigen VARCHAR(260),
  BatchId VARCHAR(100)
);

-- Índices recomendados en hechos
CREATE INDEX IX_HechosVentas_Fecha ON dbo.HechosVentas(ClaveFecha);
CREATE INDEX IX_HechosVentas_Producto ON dbo.HechosVentas(ClaveProducto);
CREATE INDEX IX_HechosVentas_Cliente ON dbo.HechosVentas(ClaveCliente);

-- Población DimTiempo 
DECLARE @inicio DATE = '2018-01-01', @fin DATE = '2030-12-31';

;WITH Fechas AS (
  SELECT @inicio AS f
  UNION ALL
  SELECT DATEADD(DAY,1,f) FROM Fechas WHERE f < @fin
)
INSERT INTO dbo.DimTiempo (ClaveFecha, FechaCompleta, Dia, Mes, NombreMes, Trimestre, Año, DiaSemana, EsFinDeSemana)
SELECT 
  CONVERT(INT, FORMAT(f,'yyyyMMdd')) AS ClaveFecha,
  f,
  DATEPART(DAY,f),
  DATEPART(MONTH,f),
  DATENAME(MONTH,f),
  DATEPART(QUARTER,f),
  DATEPART(YEAR,f),
  DATEPART(WEEKDAY,f),
  CASE WHEN DATEPART(WEEKDAY,f) IN (1,7) THEN 1 ELSE 0 END
FROM Fechas
OPTION (MAXRECURSION 0);

--- Carga de Datos --- 

USE Jardineria_DM;
GO

INSERT INTO dbo.DimCategoria (NombreCategoria, Descripcion)
SELECT DISTINCT
  LTRIM(RTRIM(Gama)) AS NombreCategoria,
  LTRIM(RTRIM(DescripcionTexto)) AS Descripcion
FROM jardineria_stg.dbo.Stg_Categoria s
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.DimCategoria d WHERE d.NombreCategoria = LTRIM(RTRIM(s.Gama))
);

-- Insertar nuevos productos
INSERT INTO dbo.DimProducto (CodigoProducto, NombreProducto, ClaveCategoria, CantidadEnStock, Precio)
SELECT 
  LTRIM(RTRIM(p.CodigoProducto)),
  LTRIM(RTRIM(p.Nombre)),
  d.ClaveCategoria,
  p.CantidadEnStock,
  p.PrecioVenta
FROM jardineria_stg.dbo.Stg_Producto p
LEFT JOIN dbo.DimCategoria d ON d.NombreCategoria = LTRIM(RTRIM(p.Gama))
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.DimProducto dp WHERE dp.CodigoProducto = LTRIM(RTRIM(p.CodigoProducto))
);

-- Actualizar los existentes (stock/precio/nombre/categoria)
UPDATE dp
SET 
  dp.NombreProducto = p.Nombre,
  dp.CantidadEnStock = p.CantidadEnStock,
  dp.Precio = p.PrecioVenta,
  dp.ClaveCategoria = d.ClaveCategoria
FROM dbo.DimProducto dp
JOIN jardineria_stg.dbo.Stg_Producto p ON dp.CodigoProducto = p.CodigoProducto
LEFT JOIN dbo.DimCategoria d ON d.NombreCategoria = LTRIM(RTRIM(p.Gama));

-- Insertar clientes nuevos (versión actual)
INSERT INTO dbo.DimCliente (ClaveClienteNatural, NombreCliente, Ciudad, Pais, LimiteCredito, VigenteDesde, VigenteHasta, EsActual)
SELECT 
  s.CodigoCliente,
  LTRIM(RTRIM(s.NombreCliente)),
  LTRIM(RTRIM(s.Ciudad)),
  LTRIM(RTRIM(s.Pais)),
  s.LimiteCredito,
  SYSUTCDATETIME(),
  NULL,
  1
FROM jardineria_stg.dbo.Stg_Cliente s
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.DimCliente dc WHERE dc.ClaveClienteNatural = s.CodigoCliente AND dc.EsActual = 1
);

-- Detectar clientes con cambios respecto a la versión actual
;WITH ClientesCambiados AS (
  SELECT s.CodigoCliente, s.NombreCliente, s.Ciudad, s.Pais, s.LimiteCredito
  FROM jardineria_stg.dbo.Stg_Cliente s
  JOIN dbo.DimCliente dc ON dc.ClaveClienteNatural = s.CodigoCliente AND dc.EsActual = 1
  WHERE 
    ISNULL(LTRIM(RTRIM(dc.NombreCliente)),'') <> ISNULL(LTRIM(RTRIM(s.NombreCliente)),'')
    OR ISNULL(LTRIM(RTRIM(dc.Ciudad)),'') <> ISNULL(LTRIM(RTRIM(s.Ciudad)),'')
    OR ISNULL(LTRIM(RTRIM(dc.Pais)),'') <> ISNULL(LTRIM(RTRIM(s.Pais)),'')
    OR ISNULL(dc.LimiteCredito,0) <> ISNULL(s.LimiteCredito,0)
)
-- Marcar versiones antiguas como no actuales
UPDATE dc
SET dc.VigenteHasta = SYSUTCDATETIME(), dc.EsActual = 0
FROM dbo.DimCliente dc
JOIN ClientesCambiados cc ON dc.ClaveClienteNatural = cc.CodigoCliente
WHERE dc.EsActual = 1;

-- Insertar nuevas versiones para los cambiados
INSERT INTO dbo.DimCliente (ClaveClienteNatural, NombreCliente, Ciudad, Pais, LimiteCredito, VigenteDesde, VigenteHasta, EsActual)
SELECT 
  s.CodigoCliente,
  LTRIM(RTRIM(s.NombreCliente)),
  LTRIM(RTRIM(s.Ciudad)),
  LTRIM(RTRIM(s.Pais)),
  s.LimiteCredito,
  SYSUTCDATETIME(),
  NULL,
  1
FROM jardineria_stg.dbo.Stg_Cliente s
JOIN ClientesCambiados cc ON s.CodigoCliente = cc.CodigoCliente;


INSERT INTO dbo.HechosVentas (ClaveFecha, ClaveCliente, ClaveProducto, ClaveCategoria, CodigoPedido, Cantidad, PrecioUnidad, NumeroLinea)
SELECT 
  CONVERT(INT, FORMAT(p.FechaPedido,'yyyyMMdd')) AS ClaveFecha,
  dc.ClaveCliente,
  dp.ClaveProducto,
  dcat.ClaveCategoria,
  p.CodigoPedido,
  dpd.Cantidad,
  dpd.PrecioUnidad,
  dpd.NumeroLinea
FROM jardineria_stg.dbo.Stg_Detalle_Pedido dpd
JOIN jardineria_stg.dbo.Stg_Pedido p ON dpd.CodigoPedido = p.CodigoPedido
LEFT JOIN dbo.DimCliente dc ON dc.ClaveClienteNatural = p.CodigoCliente AND dc.EsActual = 1
LEFT JOIN dbo.DimProducto dp ON dp.CodigoProducto = dpd.CodigoProducto
LEFT JOIN dbo.DimCategoria dcat ON dcat.NombreCategoria = (
  SELECT TOP 1 Gama FROM jardineria_stg.dbo.Stg_Producto sp WHERE sp.CodigoProducto = dpd.CodigoProducto
)
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.HechosVentas hv
  WHERE hv.CodigoPedido = dpd.CodigoPedido
    AND hv.NumeroLinea = dpd.NumeroLinea
    AND hv.ClaveProducto = dp.ClaveProducto
);


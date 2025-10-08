SELECT ClaveFecha, COUNT(*) FROM dbo.DimTiempo GROUP BY ClaveFecha HAVING COUNT(*) > 1;
SELECT NombreCategoria, COUNT(*) FROM dbo.DimCategoria GROUP BY NombreCategoria HAVING COUNT(*) > 1;
SELECT CodigoProducto, COUNT(*) FROM dbo.DimProducto GROUP BY CodigoProducto HAVING COUNT(*) > 1;
SELECT ClaveClienteNatural, EsActual, COUNT(*) FROM dbo.DimCliente GROUP BY ClaveClienteNatural, EsActual HAVING COUNT(*) > 1;

SELECT COUNT(*) FROM dbo.DimTiempo WHERE FechaCompleta IS NULL;
SELECT COUNT(*) FROM dbo.DimCliente WHERE ClaveClienteNatural IS NULL OR VigenteDesde IS NULL OR EsActual IS NULL;
SELECT COUNT(*) FROM dbo.DimProducto WHERE CodigoProducto IS NULL OR NombreProducto IS NULL;
SELECT COUNT(*) FROM dbo.HechosVentas WHERE ClaveFecha IS NULL OR ClaveCliente IS NULL OR ClaveProducto IS NULL OR CodigoPedido IS NULL;

SELECT COUNT(*) FROM dbo.HechosVentas hv LEFT JOIN dbo.DimTiempo dt ON hv.ClaveFecha = dt.ClaveFecha WHERE dt.ClaveFecha IS NULL;
SELECT COUNT(*) FROM dbo.HechosVentas hv LEFT JOIN dbo.DimCliente dc ON hv.ClaveCliente = dc.ClaveCliente WHERE dc.ClaveCliente IS NULL;
SELECT COUNT(*) FROM dbo.HechosVentas hv LEFT JOIN dbo.DimProducto dp ON hv.ClaveProducto = dp.ClaveProducto WHERE dp.ClaveProducto IS NULL;
SELECT COUNT(*) FROM dbo.DimProducto dp LEFT JOIN dbo.DimCategoria dc ON dp.ClaveCategoria = dc.ClaveCategoria WHERE dp.ClaveCategoria IS NOT NULL AND dc.ClaveCategoria IS NULL;

SELECT COUNT(*) FROM dbo.DimTiempo WHERE DiaSemana IN (2, 3, 4, 5, 6) AND EsFinDeSemana = 1;
SELECT COUNT(*) FROM dbo.DimCliente WHERE EsActual = 0 AND VigenteDesde >= VigenteHasta;
SELECT COUNT(*) FROM dbo.DimProducto WHERE Precio < 0 OR CantidadEnStock < 0;
SELECT COUNT(*) FROM dbo.HechosVentas WHERE Cantidad <= 0 OR PrecioUnidad <= 0;
SELECT COUNT(*) FROM dbo.HechosVentas WHERE MontoVenta <> (Cantidad * PrecioUnidad);

-- Consulta para verificar múltiples versiones activas (DEBE SER CERO)
SELECT ClaveClienteNatural, COUNT(*) AS VersionesActuales
FROM dbo.DimCliente
WHERE EsActual = 1
GROUP BY ClaveClienteNatural
HAVING COUNT(*) > 1;

-- Consulta para verificar registros inactivos sin fecha de fin (DEBE SER CERO)
SELECT COUNT(*) 
FROM dbo.DimCliente 
WHERE EsActual = 0 AND VigenteHasta IS NULL;

-- Consulta para detectar solapamiento de fechas (DEBE SER CERO)
SELECT t1.ClaveClienteNatural, t1.VigenteDesde, t1.VigenteHasta, t2.VigenteDesde, t2.VigenteHasta
FROM dbo.DimCliente t1
JOIN dbo.DimCliente t2 ON t1.ClaveClienteNatural = t2.ClaveClienteNatural
WHERE t1.ClaveCliente < t2.ClaveCliente -- Comparar registros diferentes
  AND t1.VigenteHasta > t2.VigenteDesde -- Solapamiento: el primero termina después que el segundo empieza
  AND t1.EsActual = 0 AND t2.EsActual = 0; -- En registros no activos

-- Consulta para verificar registros de ventas con producto pero sin categoría (DEBE SER CERO)
SELECT COUNT(*) 
FROM dbo.HechosVentas 
WHERE ClaveProducto IS NOT NULL AND ClaveCategoria IS NULL;

-- Consulta para verificar que todos los clientes en hechos son 'actuales' al momento de la carga (DEBE SER CERO)
SELECT COUNT(*)
FROM dbo.HechosVentas hv
JOIN dbo.DimCliente dc ON hv.ClaveCliente = dc.ClaveCliente
WHERE dc.EsActual = 0;

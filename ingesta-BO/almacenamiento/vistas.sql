/* Ofertas de empleo público en plazo de presentación de solicitudes. */
CREATE OR REPLACE VIEW ofertas_en_plazo_presentacion AS
SELECT fp.textual fecha_publicacion, c.tipo, c.grupo, p.denominacion, p.escala,
       o.estado, c.num_plazas, fi.textual fecha_inicio_presentacion,
       ff.textual fecha_fin_presentacion, p.cuerpo, p.subescala, c.enlace,
       c.datos_contacto, c.fuente, c.organo_convocante, c.uri_eli, c.titulo,
       c.rango, c.id_orden, fd.textual fecha_disposicion, c.plazo
FROM convocatoria c, fecha fp, fecha fd, fecha fi, fecha ff, oferta o, puesto p
WHERE c.id_fecha_inicio_presentacion <= to_char(CURRENT_DATE, 'YYYYMMDD')::integer
    AND c.id_fecha_fin_presentacion >= to_char(CURRENT_DATE, 'YYYYMMDD')::integer
    AND c.id_fecha_publicacion = fp.id
    AND c.id_fecha_disposicion = fd.id
    AND c.id_fecha_inicio_presentacion = fi.id
    AND c.id_fecha_fin_presentacion = ff.id
    AND c.id = o.id_convocatoria
    AND o.id_puesto = p.id
ORDER BY fp.id DESC, c.tipo ASC;

/* Ofertas de empleo público abiertas (en proceso). */
CREATE OR REPLACE VIEW ofertas_abiertas AS
SELECT fp.textual fecha_publicacion, c.tipo, c.grupo, p.denominacion, p.escala,
       o.estado, c.num_plazas, fi.textual fecha_inicio_presentacion,
       ff.textual fecha_fin_presentacion, p.cuerpo, p.subescala, c.enlace,
       c.datos_contacto, c.fuente, c.organo_convocante, c.uri_eli, c.titulo,
       c.rango, c.id_orden, fd.textual fecha_disposicion, c.plazo
FROM fecha fp, fecha fd, oferta o, puesto p, convocatoria c
LEFT JOIN fecha fi ON c.id_fecha_inicio_presentacion = fi.id
LEFT JOIN fecha ff ON c.id_fecha_fin_presentacion = ff.id
WHERE o.estado = 'Abierta'
    AND o.id_puesto = p.id
    AND o.id_convocatoria = c.id
    AND c.id_fecha_publicacion = fp.id
    AND c.id_fecha_disposicion = fd.id
ORDER BY fp.id DESC, c.tipo ASC;

/* Ofertas de empleo público (histórico) desde 1 de enero de 2020. */
CREATE OR REPLACE VIEW ofertas_historicas AS
SELECT fp.textual fecha_publicacion, c.tipo, c.grupo, p.denominacion, p.escala,
       o.estado, c.num_plazas, fi.textual fecha_inicio_presentacion,
       ff.textual fecha_fin_presentacion, p.cuerpo, p.subescala, c.enlace,
       c.datos_contacto, c.fuente, c.organo_convocante, c.uri_eli, c.titulo,
       c.rango, c.id_orden, fd.textual fecha_disposicion, c.plazo
FROM fecha fp, fecha fd, oferta o, puesto p, convocatoria c
LEFT JOIN fecha fi ON c.id_fecha_inicio_presentacion = fi.id
LEFT JOIN fecha ff ON c.id_fecha_fin_presentacion = ff.id
WHERE c.id_fecha_publicacion >= 20200101
    AND c.id_fecha_publicacion = fp.id
    AND c.id_fecha_disposicion = fd.id
    AND c.id = o.id_convocatoria
    AND o.id_puesto = p.id
ORDER BY fp.id DESC, c.tipo ASC;
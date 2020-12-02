/* Ofertas de empleo público en plazo de presentación de solicitudes. */
CREATE OR REPLACE VIEW ofertas_en_plazo_presentacion AS
SELECT c.organo_convocante, c.titulo, c.uri_eli, c.enlace, c.grupo, c.plazo,
       c.rango, c.id_orden, c.fuente, c.tipo, c.datos_contacto, c.num_plazas,
       fp.textual fecha_publicacion, fd.textual fecha_disposicion,
       fi.textual fecha_inicio_presentacion, ff.textual fecha_fin_presentacion,
       o.estado, p.denominacion, p.cuerpo, p.escala, p.subescala
FROM convocatoria c, fecha fp, fecha fd, fecha fi, fecha ff, oferta o, puesto p
WHERE c.id_fecha_inicio_presentacion <= to_char(CURRENT_DATE, 'YYYYMMDD')::integer
    AND c.id_fecha_fin_presentacion >= to_char(CURRENT_DATE, 'YYYYMMDD')::integer
    AND c.id_fecha_publicacion = fp.id
    AND c.id_fecha_disposicion = fd.id
    AND c.id_fecha_inicio_presentacion = fi.id
    AND c.id_fecha_fin_presentacion = ff.id
    AND c.id = o.id_convocatoria
    AND o.id_puesto = p.id;

/* Ofertas de empleo público abiertas (en proceso). */
CREATE OR REPLACE VIEW ofertas_abiertas AS
SELECT c.organo_convocante, c.titulo, c.uri_eli, c.enlace, c.grupo, c.plazo,
       c.rango, c.id_orden, c.fuente, c.tipo, c.datos_contacto, c.num_plazas,
       fp.textual fecha_publicacion, fd.textual fecha_disposicion,
       fi.textual fecha_inicio_presentacion, ff.textual fecha_fin_presentacion,
       o.estado, p.denominacion, p.cuerpo, p.escala, p.subescala
FROM fecha fp, fecha fd, oferta o, puesto p, convocatoria c
LEFT JOIN fecha fi ON c.id_fecha_inicio_presentacion = fi.id
LEFT JOIN fecha ff ON c.id_fecha_fin_presentacion = ff.id
WHERE o.estado = 'Abierta'
    AND o.id_puesto = p.id
    AND o.id_convocatoria = c.id
    AND c.id_fecha_publicacion = fp.id
    AND c.id_fecha_disposicion = fd.id;

/* Ofertas de empleo público (histórico) desde 1 de enero de 2020. */
CREATE OR REPLACE VIEW ofertas_historicas AS
SELECT c.organo_convocante, c.titulo, c.uri_eli, c.enlace, c.grupo, c.plazo,
       c.rango, c.id_orden, c.fuente, c.tipo, c.datos_contacto, c.num_plazas,
       fp.textual fecha_publicacion, fd.textual fecha_disposicion,
       fi.textual fecha_inicio_presentacion, ff.textual fecha_fin_presentacion,
       o.estado, p.denominacion, p.cuerpo, p.escala, p.subescala
FROM fecha fp, fecha fd, oferta o, puesto p, convocatoria c
LEFT JOIN fecha fi ON c.id_fecha_inicio_presentacion = fi.id
LEFT JOIN fecha ff ON c.id_fecha_fin_presentacion = ff.id
WHERE c.id_fecha_publicacion >= 20200101
    AND c.id_fecha_publicacion = fp.id
    AND c.id_fecha_disposicion = fd.id
    AND c.id = o.id_convocatoria
    AND o.id_puesto = p.id;
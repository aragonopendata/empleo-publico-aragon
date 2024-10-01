from opentelemetry import trace

# Obtener el proveedor de trazas actual
tracer_provider = trace.get_tracer_provider()

# Eliminar todos los procesadores de span
for processor in tracer_provider.active_span_processor:
    tracer_provider.remove_span_processor(processor)

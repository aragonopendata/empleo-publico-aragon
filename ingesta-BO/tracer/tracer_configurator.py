from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path=env_path)
class TracerConfigurator:
    def __init__(self, dag_id: str):
        self.service_name = os.getenv("SERVICE_NAME")
        self.dag_id = dag_id
        self.endpoint = os.getenv("APM_OTLP_ENDPOINT")
        self.tracer = self._setup_tracer()

        RequestsInstrumentor().instrument()
        LoggingInstrumentor().instrument(set_logging_format=True)

    def _setup_tracer(self):
        resource = Resource.create({
            SERVICE_NAME: self.service_name,
            "dag.id": self.dag_id
        })
        provider = TracerProvider(resource=resource)
        trace.set_tracer_provider(provider)
        
        exporter = OTLPSpanExporter(
            endpoint=self.endpoint,
            insecure=True
        )
        span_processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(span_processor)
        
        return trace.get_tracer(self.service_name)

    def get_tracer(self):
        return self.tracer

    def start_span(self, name: str):
        """Inicia un nuevo span con el nombre dado."""
        return self.tracer.start_as_current_span(name)

    def set_span_status(self, span, status_code):
        """Configura el estado de un span existente."""
        span.set_status(status_code)

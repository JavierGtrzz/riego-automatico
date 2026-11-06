// Sketch del Arduino (SIN CAMBIOS respecto a la versión anterior).
// Sensor capacitivo en A0, relé en D7 (conectado al canal IN4 del módulo).
// Imprime "H:<humedad>" cada 2 s y acepta el comando "R<segundos>".

const int PIN_SENSOR = A0;
const int PIN_RELE   = 7;      // D7 -> IN4 del módulo de relés

// Calibra estos dos valores con tu sensor
const int RAW_SECO   = 600;    // lectura con el sensor en el aire
const int RAW_MOJADO = 270;    // lectura con el sensor en agua

const bool RELE_ACTIVO_BAJO = true;
const unsigned long INTERVALO_LECTURA = 2000;
const unsigned long RIEGO_MAX_MS = 10000;

unsigned long ultimaLectura = 0;
unsigned long inicioRiego = 0;
unsigned long duracionRiego = 0;
bool regando = false;
String entrada = "";

void releOn()  { digitalWrite(PIN_RELE, RELE_ACTIVO_BAJO ? LOW : HIGH); }
void releOff() { digitalWrite(PIN_RELE, RELE_ACTIVO_BAJO ? HIGH : LOW); }

float leerHumedad() {
  int raw = analogRead(PIN_SENSOR);
  float h = (float)(RAW_SECO - raw) / (RAW_SECO - RAW_MOJADO) * 100.0;
  return constrain(h, 0.0, 100.0);
}

void procesarComando(String cmd) {
  if (cmd.length() > 1 && cmd[0] == 'R' && !regando) {
    unsigned long ms = cmd.substring(1).toInt() * 1000UL;
    if (ms > 0) {
      duracionRiego = min(ms, RIEGO_MAX_MS);
      inicioRiego = millis();
      regando = true;
      releOn();
      Serial.println("EVT:RIEGO_ON");
    }
  }
}

void setup() {
  releOff();
  pinMode(PIN_RELE, OUTPUT);
  Serial.begin(9600);
}

void loop() {
  if (millis() - ultimaLectura >= INTERVALO_LECTURA) {
    ultimaLectura = millis();
    Serial.print("H:");
    Serial.println(leerHumedad(), 1);
  }

  while (Serial.available()) {
    char c = Serial.read();
    if (c == '\n') {
      entrada.trim();
      procesarComando(entrada);
      entrada = "";
    } else {
      entrada += c;
    }
  }

  if (regando && millis() - inicioRiego >= duracionRiego) {
    releOff();
    regando = false;
    Serial.println("EVT:RIEGO_OFF");
  }
}

# OrderFlow

Sistema de procesamiento de pedidos basado en microservicios, construido con Java 17 y Spring Boot 3.2.5. Los servicios se comunican de forma asíncrona mediante Apache Kafka y cada uno mantiene su propia base de datos PostgreSQL, siguiendo el patrón de base de datos por servicio.

## Arquitectura

```
                    ┌──────────────────────────────────────────────────────────────┐
                    │                          Kafka / ZooKeeper                    │
                    │  topics: order-created · order-cancelled ·                    │
                    │          stock-reserved · stock-rejected                      │
                    └────┬─────────────────┬──────────────────┬─────────────────────┘
                         │                 │                  │
                 order-created        order-cancelled   order-created
                 order-cancelled      stock-reserved    order-cancelled
                         │                 │            stock-reserved
                         ▼                 ▼                  ▼
              ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
              │  orders      │    │  inventory   │    │ notification │
              │  service     │    │  service     │    │  service     │
              └─────┬────────┘    └─────┬────────┘    └─────┬────────┘
                    │                   │                    │
              ┌─────▼────────┐   ┌──────▼───────┐   ┌───────▼────────┐   ┌────────────┐
              │ postgres-    │   │ postgres-    │   │ postgres-      │   │  user-     │
              │ orders       │   │ inventory    │   │ notification   │   │  service   │
              │ (5432)       │   │ (5433)       │   │ (5434)         │   │  (REST)    │
              └──────────────┘   └──────────────┘   └────────────────┘   │ postgres-  │
                                                                         │ users (5435)│
                                                                         └─────┬──────┘
```

| Módulo | Puerto huésped | Puerto interno | Base de datos | Rol en el flujo |
|---|---:|---:|---|---|
| `user-service` | 8084 | 8080 | users | Autenticación JWT, registro y perfiles de usuario (REST síncrono) |
| `orders-service` | 8081 | 8080 | orders | Creación, consulta y cancelación de pedidos; publica eventos |
| `inventory-service` | 8082 | 8080 | inventory | Reserva / liberación de stock con lock pesimista |
| `notification-service` | 8083 | 8080 | notification | Persiste notificaciones por eventos |
| Kafka / Zookeeper | 9092 / 2181 | 29092 / 2181 | — | Broker de mensajes (topics auto-creados) |
| PostgreSQL × 4 | 5435, 5432, 5433, 5434 | 5432 | — | Una instancia por servicio |

## Tecnologías

- Java 17, Spring Boot 3.2.5 (Spring Web, Data JPA, Validation, Security)
- Maven (multi-módulo), MapStruct, Resilience4j
- Apache Kafka (Spring Kafka), JSON como serialización
- PostgreSQL 16, H2 (solo en tests)
- JWT (jjwt) para autenticación
- Docker Compose, GitHub Actions

## Flujo de negocio

1. Un usuario autenticado crea un pedido en `orders-service` (`POST /api/orders`). Se persiste en estado `PENDING` y se publica `order-created`.
2. `inventory-service` consume `order-created`, agrega la demanda por producto, y con **lock pesimista** reserva stock (o rechaza si no hay suficiente). Publica `stock-reserved` o `stock-rejected`.
3. `orders-service` consume `stock-reserved` → `CONFIRMED`; `stock-rejected` → `CANCELLED`.
4. `notification-service` consume los eventos y persiste notificaciones.
5. Al cancelar un pedido (`PUT /api/orders/{id}/cancel`), se publica `order-cancelled` y `inventory-service` **libera las reservas** del pedido.

## Inicio rápido

Requisitos: Docker y Docker Compose.

```bash
docker compose up --build -d
```

Esto levanta las 4 bases de datos, Kafka + Zookeeper y los 4 microservicios. Los contenedores esperan a que sus dependencias estén sanas (healthchecks) antes de arrancar.

| Servicio | URL |
|---|---|
| user-service | http://localhost:8084 |
| orders-service | http://localhost:8081 |
| inventory-service | http://localhost:8082 |
| notification-service | http://localhost:8083 |
| Health (todos) | `/actuator/health` |

### Datos precargados (seed)

- **Usuarios:** `admin` y `user`, ambos con contraseña `password` (rol `ROLE_ADMIN` y `ROLE_USER`).
- **Productos/Stock:** 10 productos con stock inicial (productos 1–10: 50, 200, 150, 30, 100, 80, 120, 200, 500, 300 unidades; reservas 0).

> `inventory-service` y `notification-service` usan `ddl-auto=create` en compose: al reiniciar con `down -v` se restauran el esquema y el seed.

## Autenticación (JWT)

Todos los endpoints de negocio requieren el header `Authorization: Bearer <token>`, salvo `/api/auth/register` y `/api/auth/login`.

El token se obtiene al registrarse o iniciar sesión e incluye los claims `sub` (username), `userId` y `role`. La clave (`JWT_SECRET`) es compartida entre `user-service` y `orders-service` (HS256, al menos 32 bytes).

Respuestas de error uniformes (JSON), gestionadas por `GlobalExceptionHandler`:

```json
{ "timestamp": "2026-09-24T00:00:00", "status": 400, "message": "..." }
```

En validaciones se devuelve un mapa `errors` con los campos fallidos. Códigos principales: `400` (validación/tipo/JSON), `401` (no autenticado o credenciales inválidas), `403` (sin permisos o pedido ajeno), `404`, `405`, `409` (duplicado / conflicto de estado), `415`, `500`.

## API

### user-service (puerto 8084, prefijo `/api`)

| Método | Ruta | Acceso | Descripción |
|---|---|---|---|
| POST | `/api/auth/register` | público | Registra usuario `{username, email, password}` → `201` |
| POST | `/api/auth/login` | público | `{username, password}` → `200` con `{token, username, role}` |
| GET | `/api/users/me` | `ROLE_USER` | Perfil del usuario autenticado |
| GET | `/api/users/{id}` | `ROLE_USER` | Perfil por id (`400` si no es numérico, `404` si no existe) |
| GET | `/api/users` | `ROLE_ADMIN` | Lista de usuarios |

`RegisterRequest`: `username` (3-50), `email` válido, `password` (≥ 6). Falla con `409` si el username o el email ya existen.

`UserResponse`: `{id, username, email, role, enabled, createdAt}`.

### orders-service (puerto 8081, prefijo `/api/orders`)

| Método | Ruta | Acceso | Descripción |
|---|---|---|---|
| POST | `/api/orders` | `ROLE_USER` | Crea pedido: `{items: [{productId, quantity}]}` → `201` (estado `PENDING`); `400` si `items` está vacío o contiene cantidades no positivas |
| GET | `/api/orders/{id}` | `ROLE_USER` | Detalle del pedido; `400` no numérico, `403` pedido de otro usuario, `404` no existe |
| GET | `/api/orders` | `ROLE_USER` | Lista de pedidos del usuario autenticado |
| PUT | `/api/orders/{id}/cancel` | `ROLE_USER` | Cancela un pedido activo → `200` (`CANCELLED`); `403` pedido ajeno, `404` no existe, `409` si ya está cancelado |

`OrderResponse`: `{id, customerId, username, status, total, items: [{id, productId, quantity, price}], createdAt, updatedAt}`. Estados: `PENDING`, `CONFIRMED`, `CANCELLED`.

### inventory-service (puerto 8082, prefijo `/api/inventory`)

| Método | Ruta | Acceso | Descripción |
|---|---|---|---|
| GET | `/api/inventory/{productId}/stock` | público | Disponibilidad real (disponible menos reservado) → `200` `{productId, availableQuantity}` |

### notification-service (puerto 8083)

No expone API HTTP; consume eventos de Kafka y persiste notificaciones (`Notification`) con tipo (`ORDER_CREATED`, `ORDER_CANCELLED`, `ORDER_CONFIRMED`) y destinatario (`customer-{id}@orderflow.com` / `inventory@orderflow.com`).

## Mensajería (Kafka)

| Topic | Publicador | Consumidores | Payload |
|---|---|---|---|
| `order-created` | orders | inventory, notification | `{orderId, customerId, username, items[{productId, quantity}], timestamp}` |
| `order-cancelled` | orders | inventory, notification | igual |
| `stock-reserved` | inventory | orders, notification | `{orderId, reason, timestamp}` |
| `stock-rejected` | inventory | orders | `{orderId, reason, timestamp}` |

Los topics se auto-crean y los mensajes usan el `orderId` como clave (key). La serialización es JSON sin cabeceras de tipo.

## Ejemplos con curl

```bash
# Login de usuario
TOKEN=$(curl -s -X POST http://localhost:8084/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"user","password":"password"}' | jq -r .token)

# Crear pedido (2× producto 1 y 3× producto 2)
curl -s -X POST http://localhost:8081/api/orders \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"items":[{"productId":1,"quantity":2},{"productId":2,"quantity":3}]}'

# Consultar el pedido
curl -s http://localhost:8081/api/orders/1 -H "Authorization: Bearer $TOKEN"

# Cancelar el pedido (libera stock)
curl -s -X PUT http://localhost:8081/api/orders/1/cancel -H "Authorization: Bearer $TOKEN"

# Disponibilidad de un producto
curl -s http://localhost:8082/api/inventory/1/stock
```

## Desarrollo local

Requisitos: JDK 17 y Maven.

```bash
# Compilar y ejecutar los 74 tests de los 4 módulos
mvn -ntp clean test

# Build de un único servicio (con sus dependencias)
mvn -pl user-service -am clean package -DskipTests
```

Si no tienes Maven/JDK local, se puede verificar en contenedor:

```bash
docker run --rm -v "$PWD":/app -v orderflow-m2:/root/.m2 -w /app \
  eclipse-temurin:17-jdk \
  sh -c 'apt-get update -qq && apt-get install -y -qq maven && mvn -B -ntp clean test'
```

### Tests

| Módulo | Test | Estrategia |
|---|---|---|
| user-service | 28 | `@SpringBootTest` + MockMvc + H2 (integración), `@DataJpaTest`, Mockito |
| orders-service | 32 | `@WebMvcTest`, `@DataJpaTest`, Mockito (sin Kafka) |
| inventory-service | 9 | Mockito, `@DataJpaTest` |
| notification-service | 5 | Mockito, `@DataJpaTest` |

Los tests usan el perfil `test` (H2 en memoria) y cubren: validación de tokens JWT (firma, expiración, `alg=none`, formato), controladores con autorización por rol/ownership, la reserva de stock con agregación de productos duplicados y lock pesimista, la cancelación atómica (`update ... where status <> :status`) y el mapeo JPA.

## CI (GitHub Actions)

Workflow en `.github/workflows/ci.yml`, en `push` y `pull_request` a `main`:

1. **Job `test`:** Java 17 (Temurin) con caché de Maven; `mvn -B -ntp clean verify` sobre los 4 módulos. Reportes de Surefire subidos como artefacto (incluso si falla).
2. **Job `docker`:** tras `test`, construye las 4 imágenes de los servicios (`docker build -f <svc>/Dockerfile`).

## Estructura del repositorio

```
order-flow/
├── pom.xml                     # Parent multi-módulo
├── docker-compose.yml          # Infraestructura + servicios
├── .github/workflows/ci.yml    # CI
├── user-service/               # Autenticación y usuarios (8084)
├── orders-service/             # Pedidos y eventos (8081)
├── inventory-service/          # Stock y reservas (8082)
├── notification-service/       # Notificaciones por evento (8083)
└── .gitignore
```
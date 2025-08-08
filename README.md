# Backend Rinha 2025 - Submission

## About the Project

I am Rafael Vieira Ferreira, an Information Systems student and intern, and I participated in the Backend Rinha 2025 with the main goal of learning and experimenting with new technologies.

The central idea was to build a payment intermediary with a **custom load balancer** written in Go. The load balancer distributes requests between two backend instances. In the best-case scenario, the payment is persisted directly; in the worst case, it goes into a **retry queue** to be processed as soon as a processor becomes available. A **health checker** monitors both processors to avoid sending requests to unavailable instances.

In the backend, I used **reactive programming** and **Java 21 virtual threads** to optimize resource usage, along with **GraalVM** to generate native binaries. **Redis** was used both as storage and as a messaging mechanism, combined with **Kryo** for serialization and **ShedLock** to control scheduled tasks.

---

## Technologies Used

* **Language:** Java 21 + Go
* **Framework:** Spring Boot + WebFlux
* **Load Balancer:** Go (custom built)
* **Storage:** Redis
* **Messaging:** Redis
* **Serialization:** Kryo
* **Scheduling:** ShedLock
* **Native Build:** GraalVM

---

## Code Repository

[https://github.com/rafaelviefe/payment-intermediary](https://github.com/rafaelviefe/payment-intermediary)

---

## Running the Project

The `docker-compose.yml` is configured to spin up:

* **Redis**
* **API** (default processor)
* **Load Balancer**

Just run:

```bash
docker compose up --build
```

The service will be available on port **9999**.

---

## Notes

The focus of this project was not only performance but also learning: from writing a load balancer from scratch to using virtual threads and native binaries with GraalVM. It was a great exercise in technology integration and resource optimization.

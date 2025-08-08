# Rinha de Backend 2025 - Submissão

## Sobre o Projeto

Sou Rafael Vieira Ferreira, estudante de Sistemas de Informação e estagiário, e participei da Rinha de Backend 2025 com o objetivo principal de aprender e experimentar novas tecnologias.

A ideia central foi construir um intermediador de pagamentos com um **load balancer próprio** escrito em Go, pensado para ser extremamente leve e focado no que precisava: garantir o maior *fire-and-forget* possível para reduzir o tempo médio de resposta.

O load balancer distribui requisições entre duas instâncias do backend. No melhor cenário, o pagamento é persistido diretamente; no pior, ele vai para uma **fila de retentativas** para ser processado assim que algum processador estiver disponível. Um **health checker** monitora os dois processadores para evitar o envio de requisições para instâncias indisponíveis.

No backend, usei **programação reativa** e **virtual threads do Java 21** para otimizar consumo de recursos, além de **GraalVM** para gerar binários nativos. O **Redis** foi usado como armazenamento e também como mecanismo de mensageria, junto com **Kryo** para serialização e **ShedLock** para controlar tarefas agendadas.

---

## Tecnologias Utilizadas

* **Linguagem:** Java 21 + Go
* **Framework:** Spring Boot + WebFlux
* **Load Balancer:** Go (desenvolvido por mim)
* **Armazenamento:** Redis
* **Mensageria:** Redis
* **Serialização:** Kryo
* **Agendamento:** ShedLock
* **Build Nativo:** GraalVM

---

## Repositório do Código

[https://github.com/rafaelviefe/intermediador-de-pagamentos](https://github.com/rafaelviefe/intermediador-de-pagamentos)

---

## Execução

O `docker-compose.yml` está configurado para subir:

* **Redis**
* **API 1**
* **API 2**
* **Load Balancer**

Basta executar:

```bash
docker compose up --build
```

O serviço ficará disponível na porta **9999**.

---

## Observações

O foco desse projeto não foi apenas performance, mas também aprendizado: desde escrever um load balancer do zero, até usar virtual threads e binários nativos com GraalVM. Foi um ótimo exercício de integração de tecnologias e otimização de recursos.

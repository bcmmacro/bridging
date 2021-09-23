# bridging
We have two datacenters, one is private and another is cloud. Mainly for security/privary concerns, we are not allowed to have inbound connections to our private DC. Also we should not transfer sensitive data to cloud. So we come up with this solution: cloud serves the website frontend (nodejs like), a bridge forwards requests from frontend / replies from private DC.

## How it works
Bridge is the one running on cloud, Gateway running on private DC.
Bridge listens to a special websocket "/bridge" which is supposed to be connected by Gateway.
Bridge wraps HTTP and other websocket requests, forwards them over "/bridge" to Gateway.
Gateway unwraps the requests and routes them to the correct downstream services, routing is done with a special header "bridging-base-url" from frontend.

## Usecases
1. You are going to serve a public website, but critical business services are running on-premise. and You are not allowed to have inbound connections to on-premise infrastructure.
2. You are hosting a website. You want to use your own PC to save cloud bill.

## Securities
1. Gateway implements firewall (whitelists).
2. "/bridge" is protected with private token.

## Limitations
1. It bridges HTTP and websocket only, which is its nature and by design.
2. Messages over "/bridge" are gzipped, but duplicate traffic (into cloud) could become the bottleneck of this setup.

## For dev
Welcome reimplementation in other languages.

"""Minimal HTTP boundary. Framework access logs must be disabled by the runner."""
from aiohttp import web


def create_app(service):
    app = web.Application(client_max_size=service.config.max_body_bytes)

    async def health(request):
        return web.json_response({'ok': True})

    async def ready(request):
        return web.json_response({'ready': service.ready}, status=200 if service.ready else 503)

    async def webhook(request):
        body = bytearray()
        async for part in request.content.iter_chunked(8192):
            body.extend(part)
            if len(body) > service.config.max_body_bytes:
                return web.json_response({'error': 'body_too_large'}, status=413)
        status, response = await service.handle(request.path, request.headers, bytes(body))
        return web.json_response(response, status=status)

    async def lifespan(app):
        await service.start()
        try:
            yield
        finally:
            await service.shutdown()

    app.cleanup_ctx.append(lifespan)
    app.router.add_get('/healthz', health)
    app.router.add_get('/readyz', ready)
    for path in service.config.webhook_routes:
        app.router.add_post(path, webhook)
    return app

"""
route.py — Minimal health-check web server.
Koyeb / Railway / Render require an HTTP endpoint to confirm the service is alive.
Returns {"status": "online"} on GET /.
"""

from aiohttp import web

routes = web.RouteTableDef()


@routes.get("/", allow_head=True)
async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "online", "service": "Rimuru Rename Bot"})


async def web_server() -> web.Application:
    app = web.Application()
    app.add_routes(routes)
    return app

#!/usr/bin/env python3
import os
from http.server import SimpleHTTPRequestHandler, HTTPServer

# Serve files from this script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

class RootHandler(SimpleHTTPRequestHandler):
	def do_GET(self):
		if self.path in ("/", "/index.html"):
			self.path = "/课件.html"
		return super().do_GET()

def main() -> None:
	server_address = ("0.0.0.0", 8000)
	httpd = HTTPServer(server_address, RootHandler)
	print("Serving 课件.html on http://0.0.0.0:8000/")
	httpd.serve_forever()

if __name__ == "__main__":
	main() 
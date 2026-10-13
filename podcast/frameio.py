import base64
import re
from urllib.parse import urlsplit

import requests

QUERY = """query GetAssetForDownload($assetId: ID!, $shareId: ID!) {
  share(shareId: $shareId) { downloadingEnabled }
  asset(assetId: $assetId) {
    ... on VideoAsset {
      media {
        hlsManifest
        videoTranscodes { key encodeStatus downloadUrl filesizeInBytes }
        original { downloadUrl }
      }
    }
  }
}"""


def resolve_frameio_url(url: str) -> str:
    # shortcut: Frame.io's public web API can change; replace with an official share API when available.
    with requests.Session() as session:
        page = session.get(url, timeout=30)
        page.raise_for_status()
        parsed = urlsplit(page.url)
        match = re.fullmatch(r"/share/([\w-]+)/view/([\w-]+)/?", parsed.path)
        if parsed.hostname != "next.frame.io" or not match:
            raise ValueError("Use a Frame.io share link that opens one video.")
        share_id, asset_id = match.groups()
        response = session.post(
            "https://api.frame.io/graphql",
            headers={
                "x-frameio-share-authentication": base64.b64encode(
                    share_id.encode()
                ).decode(),
                "apollographql-client-name": "web-app",
                "x-gql-op": "GetAssetForDownload",
                "Origin": "https://next.frame.io",
                "Referer": page.url,
            },
            json={
                "operationName": "GetAssetForDownload",
                "query": QUERY,
                "variables": {"shareId": share_id, "assetId": asset_id},
            },
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()
    if result.get("errors"):
        raise ValueError(
            "Frame.io could not authorize this share or retrieve its video."
        )
    data = result.get("data") or {}
    if not (data.get("share") or {}).get("downloadingEnabled"):
        raise ValueError("Downloads are disabled for this Frame.io share.")
    media = (data.get("asset") or {}).get("media") or {}
    if media.get("hlsManifest"):
        return media["hlsManifest"]
    proxies = [
        item
        for item in media.get("videoTranscodes", [])
        if item["key"].startswith("h264_")
        and item["encodeStatus"] == "SUCCESS"
        and item.get("downloadUrl")
        and item.get("filesizeInBytes")
    ]
    if proxies:
        return min(proxies, key=lambda item: item["filesizeInBytes"])["downloadUrl"]
    if original := (media.get("original") or {}).get("downloadUrl"):
        return original
    raise ValueError("The Frame.io share has no downloadable video.")

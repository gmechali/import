# Copyright 2026 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Includes authentication utility classes.
"""

from datetime import datetime
import logging

import google.auth
import google.auth.credentials
import google.auth.jwt
import google.auth.transport.requests
from google.oauth2 import id_token


class IDTokenCredentials(google.auth.credentials.Credentials):
  """Custom credentials class to handle ID tokens with default credentials.
  
  This class wraps standard default credentials and overrides refresh/before_request
  to explicitly fetch an ID token for a specific target audience. This is needed
  because standard default credentials typically provide access tokens, not ID tokens
  for custom services.
  """
  def __init__(self, target_audience: str) -> None:
    super().__init__()
    self.target_audience = target_audience
    self.creds, _ = google.auth.default()
    self.auth_req = google.auth.transport.requests.Request()

  def refresh(self, request):
    """Fetch a fresh ID token."""
    # This will get the token for the audience.
    # It uses ADC under the hood.
    try:
      self.token = id_token.fetch_id_token(
          request, self.target_audience)
      
      # Decode the token (JWT) to get expiry.
      # We skip verification because we trust the source (we just fetched it).
      payload = google.auth.jwt.decode(self.token, verify=False)
      self.expiry = datetime.fromtimestamp(payload['exp'])
      logging.info(f"Fetched ID token, expiry: {self.expiry}")
      
    except Exception as e:
      logging.warning(f"Failed to fetch ID token: {e}")
      # Fallback to checking if creds already has id_token (typical for local gcloud)
      if self.creds and hasattr(self.creds, 'id_token') and self.creds.id_token:
         self.token = self.creds.id_token
         try:
            payload = google.auth.jwt.decode(self.token, verify=False)
            self.expiry = datetime.fromtimestamp(payload['exp'])
         except:
            self.expiry = None
      else:
         raise

  def before_request(self, request, method, url, headers):
    """Ensure token is available before request."""
    if not self.token:
      self.refresh(request)
    super().before_request(request, method, url, headers)

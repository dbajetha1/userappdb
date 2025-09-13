import requests
import json
from typing import List, Dict, Optional
import time

class OktaFactory:
    def __init__(self, base_url: str, api_token: str):
        """
        Initialize Okta factory with base URL and API token
        
        Args:
            base_url: Okta domain URL (e.g., 'https://paloaltonetworks.oktapreview.com')
            api_token: Okta API token (SSWS token)
        """
        self.base_url = base_url.rstrip('/')
        self.headers = {
            'Authorization': f'SSWS {api_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_all_active_users(self, limit: int = 10) -> List[Dict]:
        """
        Get all active users from Okta (ONLY USERS, NO APPS)
        
        Args:
            limit: Number of users per page (max 200)
            
        Returns:
            List of user objects
        """
        users = []
        url = f"{self.base_url}/api/v1/users"
        params = {
            'filter': 'status eq "ACTIVE"',
            'limit': limit
        }
        
        while url:
            try:
                response = self.session.get(url, params=params if '?' not in url else None)
                response.raise_for_status()
                
                page_users = response.json()
                users.extend(page_users)
                
                # Check for pagination
                links = response.headers.get('Link', '')
                next_url = self._parse_next_link(links)
                url = next_url
                params = None  # Clear params for subsequent requests
                
                print(f"Retrieved {len(page_users)} users (Total: {len(users)})")
                
                # Rate limiting - be respectful to Okta API
                time.sleep(0.1)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching users: {e}")
                break
            break
                
        return users

    def get_user_app_links(self, user_id: str) -> List[Dict]:
        """
        Get all application links for a specific user
        
        Args:
            user_id: Okta user ID
            
        Returns:
            List of application link objects
        """
        url = f"{self.base_url}/api/v1/users/{user_id}/appLinks"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching app links for user {user_id}: {e}")
            return []

    def get_apps_for_users(self, users: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Get assigned applications for a list of users
        
        Args:
            users: List of user objects from get_all_active_users()
            
        Returns:
            Dictionary mapping user_id to list of applications
        """
        user_apps = {}
        
        print(f"Fetching applications for {len(users)} users...")
        
        for i, user in enumerate(users):
            user_id = user['id']
            user_email = user['profile'].get('email', user_id)
            print(f"Processing user {i+1}/{len(users)}: {user_email}")
            
            apps = self.get_user_app_links(user_id)
            user_apps[user_id] = apps
            
            # Rate limiting
            time.sleep(0.1)
            
        return user_apps

    def get_user_by_id(self, user_id: str) -> Optional[Dict]:
        """
        Get a specific user by ID
        
        Args:
            user_id: Okta user ID
            
        Returns:
            User object or None if not found
        """
        url = f"{self.base_url}/api/v1/users/{user_id}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching user {user_id}: {e}")
            return None

    def _parse_next_link(self, link_header: str) -> Optional[str]:
        """
        Parse the Link header to find the next page URL
        
        Args:
            link_header: HTTP Link header value
            
        Returns:
            Next page URL or None
        """
        if not link_header:
            return None
            
        links = link_header.split(',')
        for link in links:
            if 'rel="next"' in link:
                url = link.split(';')[0].strip('<> ')
                return url
        return None

    def close(self):
        """Close the session"""
        self.session.close()
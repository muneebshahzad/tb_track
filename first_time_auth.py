import lazop
from token_manager import save_tokens, BASE_URL, APP_KEY, APP_SECRET

print("Open this URL in your browser:")
print("https://api.daraz.pk/oauth/authorize?response_type=code&redirect_uri=https://admin.tickbags.com/daraz&client_id=501554")

code = input("\nPaste the code here: ").strip().strip('"')  # strips quotes if accidentally included

client = lazop.LazopClient(BASE_URL, APP_KEY, APP_SECRET)
request = lazop.LazopRequest('/auth/token/create')
request.add_api_param('code', code)
response = client.execute(request)

body = response.body  # ← already a dict, no json.loads() needed
print(body)

save_tokens(body["access_token"], body["refresh_token"])
print("✅ Tokens saved to daraz_tokens.json")
import dotenv
vals = dotenv.dotenv_values('.env')
print(repr(vals.get('R2_ENDPOINT_URL')))
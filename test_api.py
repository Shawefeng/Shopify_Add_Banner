from retail_promotions_to_shopify_metafields import Config, ShopifyClient, require_env

def test_api():
    try:
        require_env()
        client = ShopifyClient()
        print(f'Shop: {Config.SHOPIFY_SHOP}')
        print(f'API: {Config.SHOPIFY_API_VERSION}')

        # Test basic GraphQL query
        q = '''
        query {
          collections(first: 1) {
            nodes { id title }
          }
        }
        '''

        data = client.graphql(q)
        collections = data['data']['collections']['nodes']
        print(f'Test query successful. Found {len(collections)} collections.')
        if collections:
            print(f'First collection: {collections[0]["title"]}')

    except Exception as e:
        print(f'Error: {e}')
        return False
    return True

if __name__ == "__main__":
    test_api()
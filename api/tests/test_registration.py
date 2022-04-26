ingestion_endpoint = "/ingestions"


def test_list(api_client, mock_table, example_ingestion):
    mock_table.query.return_value = {"Items": [example_ingestion]}
    response = api_client.get(ingestion_endpoint)
    assert response.status_code == 200
    assert response.json() == {"items": [example_ingestion], "next": None}

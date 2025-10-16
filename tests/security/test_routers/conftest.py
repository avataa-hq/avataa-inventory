from pytest import fixture


@fixture(scope="function", name="app")
def test_app(client):
    yield client

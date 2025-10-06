from fastapi import FastAPI

app = FastAPI()

@app.get('/')
def test_railway():
    output = {'message': 'Hello World!'}
    return output
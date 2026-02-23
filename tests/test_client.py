from source_sage_intacct.client import IntacctAuthError, IntacctClient, QueryResult


class DummyResponse:
    def __init__(self, status_code: int, text: str):
        self.status_code = status_code
        self.text = text


class DummySession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = 0

    def post(self, *_args, **_kwargs):
        response = self.responses[self.calls]
        self.calls += 1
        return response

    def close(self):
        return None


def _config():
    return {
        "sender_id": "sender",
        "sender_password": "sender-pass",
        "company_id": "company",
        "user_id": "user",
        "user_password": "user-pass",
        "api_url": "https://api.intacct.com/ia/xml/xmlgw.phtml",
    }


def test_read_by_query_parses_result_metadata():
    client = IntacctClient(_config())
    client.session = DummySession(
        [
            DummyResponse(
                200,
                """
<response>
  <control><status>success</status></control>
  <operation>
    <authentication><status>success</status></authentication>
    <result>
      <status>success</status>
      <data><GLACCOUNT><RECORDNO>1</RECORDNO></GLACCOUNT></data>
      <resultId>abc</resultId>
      <numremaining>5</numremaining>
    </result>
  </operation>
</response>
""".strip(),
            )
        ]
    )

    result = client.read_by_query("GLACCOUNT", ["*"], "RECORDNO > 0", 10)

    assert isinstance(result, QueryResult)
    assert result.result_id == "abc"
    assert result.num_remaining == 5
    assert result.records == [{"RECORDNO": "1"}]


def test_auth_failure_raises_auth_error():
    client = IntacctClient(_config())
    client.session = DummySession(
        [
            DummyResponse(
                200,
                """
<response>
  <control><status>success</status></control>
  <operation>
    <authentication>
      <status>failure</status>
      <errormessage><error><description2>Invalid login</description2></error></errormessage>
    </authentication>
  </operation>
</response>
""".strip(),
            )
        ]
    )

    try:
        client.read_by_query("GLACCOUNT", ["*"], "RECORDNO > 0", 10)
        assert False, "Expected IntacctAuthError"
    except IntacctAuthError:
        assert True


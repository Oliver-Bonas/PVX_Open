class PvxClient(object):
    WSDL_URL = 'http://wms.peoplevox.net/{0}/resources/integrationservicev4.asmx?wsdl'



    def __init__(self, client_id, username, password):
        from zeep import Client, Transport, Settings

        settings = Settings(strict=False, xml_huge_tree=True)

        client = Client(transport=Transport(timeout=None),
                             wsdl=self.WSDL_URL.format(client_id),settings = settings
                             )
        auth_response = client.service.Authenticate(client_id,
                                                    username,
                                                    password)
        session = auth_response['Detail'].split(',')[1]
        SessionCredentials = client.get_type('ns0:UserSessionCredentials')
        creds = SessionCredentials(UserId=0, ClientId=client_id, SessionId=session)
        self._client = client
        self._auth = {'_soapheaders': [creds]}

    def get_report(self, report_name, columns, sort=None, filters=None, page_num=1, page_size=0):
        GetReportRequest = self._client.get_type('ns0:GetReportRequest')
        get_report_request = GetReportRequest(
            TemplateName=report_name,
            PageNo=page_num,
            ItemsPerPage=page_size,
            OrderBy=sort,
            Columns=columns,
            SearchClause=filters
        )
        report = self._client.service.GetReportData(
            getReportRequest=get_report_request,
            **self._auth
        )
        return report
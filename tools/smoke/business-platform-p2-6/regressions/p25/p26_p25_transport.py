"""P25 transport adaptation: phase gate, before-call receipts, safe redirects.

Original scenario orchestration/assertions and return shape are retained.
"""
import hashlib,json,time
from urllib import request,error,parse


def install(engine,context):
    class SafeApi:
        def __init__(self,bases,jwt,recorder,scenario):
            self.bases=bases; self.jwt=jwt; self.rec=recorder; self.scenario=scenario
        def call(self,base_key,method,path,body=None,timeout=30,raw=False,auth=True):
            context.gate()
            if self.bases[base_key]!=context.bases[base_key]: raise ValueError('P25 HTTP origin not Root-bound')
            url=self.bases[base_key].rstrip('/')+path
            split=parse.urlsplit(url)
            if split.username or split.password or split.fragment: raise ValueError('unsafe HTTP URL')
            payload=json.dumps(body).encode() if body is not None else None
            headers={'Accept':'application/json'}
            if payload is not None: headers['Content-Type']='application/json'
            if auth and self.jwt: headers['Authorization']=self.jwt if self.jwt.lower().startswith('bearer ') else 'Bearer '+self.jwt
            receipt=context.attempt.step_begin('http',{'base':base_key,'method':method,'url':context.W.safe_url_projection(url),'request_body_sha256':hashlib.sha256(payload).hexdigest() if payload else None})
            start=time.time(); status=None; content=b''; failure=None; redirect=None
            try:
                opener=request.build_opener(context.W._NoRedirectHandler())
                req=request.Request(url,data=payload,headers=headers,method=method)
                try:
                    with opener.open(req,timeout=timeout) as response:
                        status=response.status; content=response.read(context.W.HTTP_MAX_BODY+1)
                except error.HTTPError as exc:
                    status=exc.code; content=exc.read(context.W.HTTP_MAX_BODY+1)
                    if status==302:
                        if method!='GET' or base_key!='dataset' or not path.split('?')[0].endswith('/download'):
                            raise ValueError('redirect outside exact Dataset download request')
                        follow=context.W.validate_download_redirect(exc.headers.get('Location',''))
                        redirect={'policy':'one anonymous same-origin local-fs follow','safe_url':context.W.safe_url_projection(follow)}
                        context.gate()
                        # No Authorization or previous request headers are copied.
                        with opener.open(request.Request(follow,method='GET'),timeout=timeout) as response:
                            status=response.status; content=response.read(context.W.HTTP_MAX_BODY+1)
                if len(content)>context.W.HTTP_MAX_BODY: raise ValueError('HTTP body exceeds bounded proof cap')
            except Exception as exc:
                failure=context.attempt.redact(type(exc).__name__+': '+str(exc)); content=b''
            text=content.decode('utf-8','replace')
            try: value=json.loads(text) if text and not raw else None
            except ValueError: value=None
            context.attempt.step_finish('http',receipt,content,'unavailable',{'original_rc':None,'http_status':status,'error':failure,'redirect':redirect,'response_sha256':hashlib.sha256(content).hexdigest()})
            detail={'base':base_key,'method':method,'path':url.split('?',1)[0],'query':parse.urlsplit(url).query,
                'status':status,'elapsed_ms':int((time.time()-start)*1000),'error':failure,
                'resp_sha256':hashlib.sha256(content).hexdigest() if content else None,'resp_bytes':len(content),
                'request_body_sha256':hashlib.sha256(payload).hexdigest() if payload else None}
            if not raw and text: detail['resp_body']=context.attempt.redact(text[:engine._MAX_RECORDED_BODY]+('...<truncated>' if len(text)>engine._MAX_RECORDED_BODY else ''))
            self.rec.record(self.scenario,'http',detail)
            result={'status':status,'json':content if raw else value,'raw':content,'error':failure,'text':text}
            context.observe(base_key,method,path,body,result,receipt,token=self.jwt)
            return result
    engine.HttpApi=SafeApi
    return engine

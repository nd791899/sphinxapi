-- Copyright (C) 2014 nd791899 191076013@qq.com

local bit = require "bit"
local band = bit.band
local lshift = bit.lshift
local rshift = bit.rshift



local   SPH_TRUE                = 1
local   SPH_FALSE               = 0

local	SPH_MATCH_ALL			= 0
local	SPH_MATCH_ANY			= 1
local	SPH_MATCH_PHRASE		= 2
local	SPH_MATCH_BOOLEAN		= 3
local	SPH_MATCH_EXTENDED		= 4
local	SPH_MATCH_FULLSCAN		= 5
local	SPH_MATCH_EXTENDED2		= 6



local	SPH_SORT_RELEVANCE		= 0
local	SPH_SORT_ATTR_DESC		= 1
local	SPH_SORT_ATTR_ASC		= 2
local	SPH_SORT_TIME_SEGMENTS	= 3
local	SPH_SORT_EXTENDED		= 4
local	SPH_SORT_EXPR			= 5


local	SPH_GROUPBY_DAY			= 0
local	SPH_GROUPBY_WEEK		= 1
local	SPH_GROUPBY_MONTH		= 2
local	SPH_GROUPBY_YEAR		= 3
local	SPH_GROUPBY_ATTR		= 4
local	SPH_GROUPBY_ATTRPAIR	= 5



local	SPH_RANK_PROXIMITY_BM25	= 0
local	SPH_RANK_BM25			= 1
local	SPH_RANK_NONE			= 2
local	SPH_RANK_WORDCOUNT		= 3
local	SPH_RANK_PROXIMITY		= 4
local	SPH_RANK_MATCHANY		= 5
local	SPH_RANK_FIELDMASK		= 6
local	SPH_RANK_SPH04			= 7
local	SPH_RANK_DEFAULT		= SPH_RANK_PROXIMITY_BM25

local   MAX_REQS				=32



local	SEARCHD_COMMAND_SEARCH		= 0
local	SEARCHD_COMMAND_EXCERPT		= 1
local	SEARCHD_COMMAND_UPDATE		= 2
local	SEARCHD_COMMAND_KEYWORDS	= 3
local	SEARCHD_COMMAND_PERSIST		= 4
local	SEARCHD_COMMAND_STATUS		= 5



local	SPH_ATTR_INTEGER		= 1
local	SPH_ATTR_TIMESTAMP		= 2
local	SPH_ATTR_ORDINAL		= 3
local	SPH_ATTR_BOOL			= 4
local	SPH_ATTR_FLOAT			= 5
local	SPH_ATTR_BIGINT			= 6
local	SPH_ATTR_STRING			= 7
local	SPH_ATTR_MULTI			= 0x40000001
local	SPH_ATTR_MULTI64		= 0x40000002



local sphinx ={ver_search =0x119}
local sx = { __index = sphinx }


local _sphinx_client ={ver_search =0x119}
_sphinx_client.copy_args=SPH_TRUE
_sphinx_client.head_alloc=Nil
_sphinx_client.error=Nil
_sphinx_client.warning=Nil
_sphinx_client.local_error_buf=Nil

_sphinx_client.host="localhost"
_sphinx_client.port=9312

_sphinx_client.timeout=0
_sphinx_client.offset= 0;
_sphinx_client.limit=20
_sphinx_client.mode=SPH_MATCH_ALL
_sphinx_client.num_weights=0
_sphinx_client.weights={}
_sphinx_client.sort=SPH_SORT_RELEVANCE
_sphinx_client.sortby = ""
_sphinx_client.minid = 0
_sphinx_client.maxid= 0

_sphinx_client.group_by = ""
_sphinx_client.group_func				= SPH_GROUPBY_ATTR
_sphinx_client.group_sort				= "@groupby desc"
_sphinx_client.group_distinct			= ""
_sphinx_client.max_matches				= 1000
_sphinx_client.cutoff					= 0
_sphinx_client.retry_count				= 0
_sphinx_client.retry_delay				= 0
_sphinx_client.geoanchor_attr_lat		= Nil
_sphinx_client.geoanchor_attr_long		= Nil
_sphinx_client.geoanchor_lat			= 0
_sphinx_client.geoanchor_long			= 0
_sphinx_client.num_filters				= 0
_sphinx_client.max_filters				= 0
_sphinx_client.filters					= Nil
_sphinx_client.num_index_weights		= 0
_sphinx_client.index_weights_names		= Nil
_sphinx_client.index_weights_values	= Nil
_sphinx_client.ranker					= SPH_RANK_DEFAULT
_sphinx_client.max_query_time			= 0
_sphinx_client.num_field_weights		= 0
_sphinx_client.field_weights_names		= {}
_sphinx_client.field_weights_values	= {}
_sphinx_client.num_overrides			= 0
_sphinx_client.max_overrides			= 0
_sphinx_client.overrides				= Nil
_sphinx_client.select_list				= ""

_sphinx_client.num_reqs				= 0
_sphinx_client.response_len			= 0
_sphinx_client.response_buf			= Nil
_sphinx_client.num_results				= {}





for i=1,MAX_REQS do
    result={}
    result.values_pool = Nil;
	result.words = Nil;
	result.fields = Nil;
	result.attr_names = Nil;
	result.attr_types = Nil;
	table.insert(_sphinx_client.num_results, result);
end

_sphinx_client.sock = -1;
_sphinx_client.persist = SPH_FALSE;


function send_int(value,str)
   str = str .. string.char(band(rshift(value,24),255)) .. string.char(band(rshift(value,16),255)) .. string.char(band(rshift(value,8),255)) .. string.char(band(value,255))
   return str
end

function send_word(value,str)
   str = str  .. string.char(band(rshift(value,8),255)) .. string.char(band(value,255))
   return str
end

function send_str(value,str)
   str=send_int(string.len(value),str)
   str = str  .. value
   return str
end

function send_qword(value,str)
   str=send_int ( rshift(value,32) , str );
   str=send_int ( band(value, 0xffffffff) ,str );
   return str
end

function unpack_short(value)
    val= string.byte(value,2)
	val= val + string.byte(value,2)*256
    return string.sub(value,3,#value),val
end

function unpack_int(value)
    val= string.byte(value,4)
	val= val + string.byte(value,3)*256
	val= val + string.byte(value,2)*256*256
	val= val + string.byte(value,1)*256*256*256
    return string.sub(value,5,#value),val
end

function unpack_str(value)
    local value,len= unpack_int(value)
	local val = string.sub(value,len+1,#value)
	local str = string.sub(value,1,len)
    return val,str
end

function unpack_qword(value)
    local value,high= unpack_int(value)
	local value,low= unpack_int(value)
	return value,high*2^32+low
end 


function sphinx.new(self)
    local client = _sphinx_client
	return setmetatable({ client = client },sx)
end

function sphinx.sphinx_set_server(self,host,port)
    self.client.host = host
	self.client.port = port
        self.client.sock, err = ngx.socket.tcp()
        ok, err = self.client.sock:connect(host, port)
        if not ok then
            ngx.say("connect faile!")
        end 
end

function sphinx.sphinx_set_match_mode(self,mode)
    self.client.mode = mode
end

function sphinx.sphinx_set_sort_mode(self,mode,sortby)
	self.client.mode = mode
	self.client.sortby = sortby
end

function sphinx.sphinx_set_field_weights(self,num_weights,field_names,field_weights)

	self.client.num_field_weights = num_weights
	self.client.field_weights_names = field_names
	self.client.field_weights_values = field_weights
end

function sphinx.sphinx_add_query(self,query,index_list, comment)
    req=""
    req=send_int(self.client.offset,req)
    req=send_int(self.client.limit,req)
    req=send_int(self.client.mode,req)
    req=send_int(self.client.ranker,req)
    req=send_int(self.client.sort,req)
    req=send_str(self.client.sortby,req)
    req=send_str(query,req)
    req=send_int(#self.client.weights,req)
	for i=1,#self.client.weights do
		req=send_int( self.client.weights[i],req )
	end
	
	req=send_str ( index_list,req );
	req=send_int ( 1,req ) -- id range bits
	req=send_qword (self.client.minid,req )
	req=send_qword(self.client.maxid,req )
	req=send_int(self.client.num_filters,req )
	--pass 
	
	req=send_int ( self.client.group_func,req )
	req=send_str ( self.client.group_by,req )
	req=send_int ( self.client.max_matches,req )
	req=send_str ( self.client.group_sort,req )
	req=send_int ( self.client.cutoff,req )
	req=send_int ( self.client.retry_count,req )
	req=send_int ( self.client.retry_delay,req )
	req=send_str ( self.client.group_distinct,req )
	
	--pass
	req=send_int ( 0,req )
	
	req=send_int ( self.client.num_index_weights,req )
	--pass
	
	req=send_int ( self.client.max_query_time,req )
	req=send_int ( self.client.num_field_weights,req )

	for i=1,#self.client.field_weights_names do
		req=send_str (self.client.field_weights_names[i],req );
		req=send_int ( self.client.field_weights_values[i],req );
	end
	req=send_str(comment,req);
	
	if self.client.ver_search>=0x115  then
	    req=send_int ( self.client.num_overrides,req )
		--pass
	end
	
	if self.client.ver_search>=0x116  then
	    req=send_str ( self.client.select_list,req )
		--pass
	end
	
	_sphinx_client.num_reqs = _sphinx_client.num_reqs +1
	
	return req

end

function sphinx.sphinx_query(self,query,index_list, comment)

    input=""
    input=send_int(1,"")
	
    self.client.sock:send(input)
    local input, recvt, sendt, status,response, receive_status
    response, receive_status = self.client.sock:receive(4)


    req = self.sphinx_add_query(self,query,index_list, comment);
	
	
    local len = 8 + #req
    local req_header=""
    req_header=send_word(SEARCHD_COMMAND_SEARCH,req_header)
    req_header=send_word(self.client.ver_search,req_header)
    req_header=send_int(len,req_header)
    req_header=send_int(0,req_header)
    req_header=send_int(self.client.num_reqs,req_header)

    self.client.sock:send(req_header)
    self.client.sock:send(req)

    response, receive_status = self.client.sock:receive(8)

    value, status =unpack_short(response)
    value, ver =unpack_short(value)
    value, len =unpack_int(value)

    response, receive_status = self.client.sock:receive(len)
    res = format_response(response)
    return res	

end

function format_response(response)

    thelen = string.len(response)
    for i=1,thelen do
      ngx.say(string.byte(response,i) )
    end

    res={}
    value, status = unpack_int ( response )
	res.status = status
	--pass
	
	-- fields
	value,res.num_fields = unpack_int ( value )
	
	res.fields={}
	for i=1,res.num_fields do
	    value,str= unpack_str(value)
	end
	
	value, res.num_attrs = unpack_int ( value )
	
	res.attr_names = {}
	res.attr_types = {}
	for i=1,res.num_attrs do
	    value,res.attr_names[i] = unpack_str ( value )
		value,res.attr_types[i] = unpack_int ( value )
	end
	
	value, res.num_matches = unpack_int ( value )
	
	value,id64 = unpack_int ( value )
	
	res.matches ={}
	for i=1,res.num_matches do
	    res.matches[i]={}
	    if ( id64 ~= 0 ) then
		    value,res.matches[i].doc = unpack_qword ( value )
	    else
			value,res.matches[i].doc = unpack_int ( value )
	    end
		
		-- weight
		value,res.matches[i].weight = unpack_int( value )
		res.matches[i].attr={}
		for j=1,res.num_attrs do
		    res.matches[i].attr[j]={}
		    if res.attr_types[i] == SPH_ATTR_MULTI64 or res.attr_types[i] == SPH_ATTR_MULTI then
			    z=1
			elseif res.attr_types[i] == SPH_ATTR_FLOAT then
			    value,res.matches[i].attr[j].value = unpack_int ( value )
			elseif res.attr_types[i] == SPH_ATTR_BIGINT then
			    value,res.matches[i].attr[j].value = unpack_qword ( value )
			elseif res.attr_types[i] == SPH_ATTR_STRING then
			    value,res.matches[i].attr[j].value = unpack_str ( value )
			else
			    value,res.matches[i].attr[j].value = unpack_int ( value )
				
			end
		end
		
		
	end

    -- totals
	value,res.total = unpack_int ( value )
	value,res.total_found = unpack_int ( value )
	value,res.time_msec = unpack_int ( value )
	value,res.num_words = unpack_int ( value )

    res.words={}
	
	-- words
	
	for i=1,res.num_words do
	    res.words[i]={}
		value,res.words[i].word = unpack_str ( value )
		value,res.words[i].docs = unpack_int ( value )
		value,res.words[i].hits = unpack_int ( value )
	end
	
	return res
	
end

query="qq"
local client, err = sphinx.new()
client:sphinx_set_server("192.168.40.29",9312)
client:sphinx_set_match_mode( SPH_MATCH_EXTENDED2 )
client:sphinx_set_field_weights( 2, {"title","content"}, {500,1} )
res=client:sphinx_query(query,"*","")

ngx.say(string.format("Query '%s' retrieved %d of %d matches.\n",query, res.total, res.total_found))
ngx.say ( "Query stats:\n" )
for  i=1,res.num_words do
	ngx.say ( string.format("\t'%s' found %d times in %d documents\n",
	res.words[i].word, res.words[i].hits, res.words[i].docs) )
end

ngx.say ( "\nMatches:\n" );
	
for i=1, res.num_matches do
	ngx.say ( "\n" );
	ngx.say ( string.format("%d. doc_id=%d, weight=%d", i,res.matches[i].doc,res.matches[i].weight) )
	ngx.say ( "attrs:" );
	for j=1,res.num_attrs do
		   ngx.say(res.attr_names[j],res.matches[i].attr[j].value)
	end
		
end

--ngx.say("hello,lua-sphinx")
--ngx.say("hello,lua-sphinx")

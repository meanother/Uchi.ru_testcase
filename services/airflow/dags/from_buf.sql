INSERT into eventdata.events select
    toDateTime(ts/1000)
    , userId
    , sessionId
    , page
    , auth
    , method
    , status
    , level
    , itemInSession
    , userAgent
    , location
    , lastName
    , firstName
    , registration
    , gender
    , artist
    , song
    , length
from eventdata.events_buf
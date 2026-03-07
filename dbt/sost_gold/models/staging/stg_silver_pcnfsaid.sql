select *
from {{ source('dw_public', 'silver_pcnfsaid') }}

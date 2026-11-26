select extraction_key, ocr_confidence, page_number
from {{ ref('fct_unstructured_extractions') }}
where ocr_confidence < 0
   or ocr_confidence > 1
   or page_number < 1

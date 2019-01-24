``cassoldra.encoder`` - Encoders for non-prepared Statements
============================================================

.. module:: cassoldra.encoder

.. autoclass:: Encoder ()

   .. autoattribute:: cassoldra.encoder.Encoder.mapping

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_none ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_object ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_all_types ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_sequence ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_str ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_unicode ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_bytes ()

      Converts strings, buffers, and bytearrays into CQL blob literals.

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_datetime ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_date ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_map_collection ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_list_collection ()

   .. automethod:: cassoldra.encoder.Encoder.cql_encode_set_collection ()

   .. automethod:: cql_encode_tuple ()

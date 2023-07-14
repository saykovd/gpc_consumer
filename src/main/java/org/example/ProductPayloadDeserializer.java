package org.example;

import com.betfair.platform.spcs.consumer.logic.SPCSDeserRegistry;
import com.betfair.platform.spcs.domain.Catalog;
import com.betfair.platform.spcs.domain.utils.MessageType;
import com.betfair.platform.spcs.messages.SportsCatalogProto;
import com.betfair.platform.spcs.serializers.codec.CatalogCodec;
import com.betfair.platform.spcs.serializers.utils.SpcsHeaderUtils;
import com.betfair.streams.messaging.serialization.Message;
import com.betfair.streams.messaging.serialization.MessageDeserializer;
import com.betfair.streams.messaging.serialization.header.BagHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ProductPayloadDeserializer implements Deserializer<SportsCatalogProto.Catalog> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProductPayloadDeserializer.class);
    private final MessageDeserializer<BagHeader, Message> deserializer = SPCSDeserRegistry.SPCS_CODEX_REG;

    private static int countDes=0;
    @Override
    public SportsCatalogProto.Catalog deserialize(String topic, byte[] data) {

        //LOGGER.info("Des count {}",   countDes++);
        if (HeartbeatHelper.isHeartBeat(data)) {
            MessageHeaders protocolHeaders = new MessageHeaders();
            Map.of("TYPE", "HeartBeat").forEach(protocolHeaders::addHeader);
            return null;
        }

        ByteBuffer payload = ByteBuffer.wrap(data);
        BagHeader bagHeader = deserializer.getHeader(payload);
        String messageType = SpcsHeaderUtils.getMessageType(bagHeader);
        Map<String, String> headers = convertHeaders(bagHeader);

        String jsonPayload = "{}";
        if (MessageType.isCatalogMessageType(messageType)) {
            Catalog catalogMessage = (Catalog) deserializer.deserialize(payload);
            SportsCatalogProto.Catalog catalogProto = CatalogCodec.toProto(catalogMessage);
            return catalogProto;
        }
        return null;
    }

    private Map<String, String> convertHeaders(BagHeader bagHeader) {
        return new HashMap<>(Map.of(
                "SV", bagHeader.get("SV").toString(),
                "SPT", bagHeader.get("SPT").toString(),
                "MT", bagHeader.get("MT").toString(),
                "DM", bagHeader.get("DM").toString(),
                "SPU", bagHeader.get("SPU").toString(),
                "DO", bagHeader.get("DO").toString(),
                "PK", bagHeader.get("PK").toString(),
                "SPI", bagHeader.get("SPI").toString()
        ));
    }

}

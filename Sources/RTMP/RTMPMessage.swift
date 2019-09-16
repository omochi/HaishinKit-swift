import AVFoundation

enum RTMPMessageType: UInt8 {
    case chunkSize = 0x01
    case abort = 0x02
    case ack = 0x03
    case user = 0x04
    case windowAck = 0x05
    case bandwidth = 0x06
    case audio = 0x08
    case video = 0x09
    case amf3Data = 0x0F
    case amf3Shared = 0x10
    case amf3Command = 0x11
    case amf0Data = 0x12
    case amf0Shared = 0x13
    case amf0Command = 0x14
    case aggregate = 0x16

    func makeMessage() -> RTMPMessage {
        switch self {
        case .chunkSize:
            return RTMPSetChunkSizeMessage()
        case .abort:
            return RTMPAbortMessge()
        case .ack:
            return RTMPAcknowledgementMessage()
        case .user:
            return RTMPUserControlMessage()
        case .windowAck:
            return RTMPWindowAcknowledgementSizeMessage()
        case .bandwidth:
            return RTMPSetPeerBandwidthMessage()
        case .audio:
            return RTMPAudioMessage()
        case .video:
            return RTMPVideoMessage()
        case .amf3Data:
            return RTMPDataMessage(objectEncoding: 0x03)
        case .amf3Shared:
            return RTMPSharedObjectMessage(objectEncoding: 0x03)
        case .amf3Command:
            return RTMPCommandMessage(objectEncoding: 0x03)
        case .amf0Data:
            return RTMPDataMessage(objectEncoding: 0x00)
        case .amf0Shared:
            return RTMPSharedObjectMessage(objectEncoding: 0x00)
        case .amf0Command:
            return RTMPCommandMessage(objectEncoding: 0x00)
        case .aggregate:
            return RTMPAggregateMessage()
        }
    }
}

protocol RTMPMessage: CustomDebugStringConvertible {
    var type: RTMPMessageType { get }
    var length: Int { get set }
    var streamId: UInt32 { get set }
    var timestamp: UInt32 { get set }
    var payload: Data { get set }

    func execute(_ connection: RTMPConnection, type: RTMPChunkType)
}

extension RTMPMessage {
    func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
    }
}

extension RTMPMessage {
    // MARK: CustomDebugStringConvertible
    var debugDescription: String {
        return Mirror(reflecting: self).debugDescription
    }
}

// MARK: -
/**
 5.4.1. Set Chunk Size (1)
 */
struct RTMPSetChunkSizeMessage: RTMPMessage {
    let type: RTMPMessageType = .chunkSize
    var length: Int = 0
    var streamId: UInt32 = 0
    var timestamp: UInt32 = 0
    var size: UInt32 = 0

    var payload: Data {
        get {
            return size.bigEndian.data
        }
        set {
            size = UInt32(data: newValue).bigEndian
        }
    }

    init() {
    }

    init(_ size: UInt32) {
        self.size = size
    }

    func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
        connection.socket.chunkSizeC = Int(size)
    }
}

// MARK: -
/**
 5.4.2. Abort Message (2)
 */
struct RTMPAbortMessge: RTMPMessage {
    let type: RTMPMessageType = .abort
    var length: Int = 0
    var streamId: UInt32 = 0
    var timestamp: UInt32 = 0
    var size: UInt32 = 0
    var chunkStreamId: UInt32 = 0

    var payload: Data {
        get {
            return chunkStreamId.bigEndian.data
        }
        set {
            chunkStreamId = UInt32(data: newValue).bigEndian
        }
    }
}

// MARK: -
/**
 5.4.3. Acknowledgement (3)
 */
struct RTMPAcknowledgementMessage: RTMPMessage {
    let type: RTMPMessageType = .ack
    var length: Int = 0
    var streamId: UInt32 = 0
    var timestamp: UInt32 = 0
    var size: UInt32 = 0
    var sequence: UInt32 = 0

    var payload: Data {
        get {
            return sequence.bigEndian.data
        }
        set {
            sequence = UInt32(data: newValue).bigEndian
        }
    }

    init() {
    }

    init(_ sequence: UInt32) {
        self.sequence = sequence
    }
}

// MARK: -
/**
 5.4.4. Window Acknowledgement Size (5)
 */
struct RTMPWindowAcknowledgementSizeMessage: RTMPMessage {
    let type: RTMPMessageType = .windowAck
    var length: Int = 0
    var streamId: UInt32 = 0
    var timestamp: UInt32 = 0
    var size: UInt32 = 0

    var payload: Data {
        get {
            return size.bigEndian.data
        }
        set {
            size = UInt32(data: newValue).bigEndian
        }
    }

    init() {
    }

    init(_ size: UInt32) {
        self.size = size
    }

    func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
        connection.windowSizeC = Int64(size)
        connection.windowSizeS = Int64(size)
    }
}

// MARK: -
/**
 5.4.5. Set Peer Bandwidth (6)
 */
final class RTMPSetPeerBandwidthMessage: RTMPMessage {
    enum Limit: UInt8 {
        case hard = 0x00
        case soft = 0x01
        case dynamic = 0x02
        case unknown = 0xFF
    }

    let type: RTMPMessageType = .bandwidth
    var length: Int = 0
    var streamId: UInt32 = 0
    var timestamp: UInt32 = 0
    var size: UInt32 = 0
    var limit: Limit = .hard

    var payload: Data {
        get {
            var payload = Data()
            payload.append(size.bigEndian.data)
            payload.append(limit.rawValue)
            return payload
        }
        set {
            size = UInt32(data: newValue[0..<4]).bigEndian
            limit = Limit(rawValue: newValue[4]) ?? .unknown
        }
    }

    func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
        connection.bandWidth = size
    }
}

// MARK: -
/**
 7.1.1. Command Message (20, 17)
 */
final class RTMPCommandMessage: RTMPMessage {
    let type: RTMPMessageType
    var length: Int = 0
    var streamId: UInt32 = 0
    var timestamp: UInt32 = 0
    let objectEncoding: UInt8
    var commandName: String = ""
    var transactionId: Int = 0
    var commandObject: ASObject?
    var arguments: [Any?] = []

    var payload: Data {
        get {
            defer {
                serializer.clear()
            }
            if type == .amf3Command {
                serializer.writeUInt8(0)
            }
            serializer
                .serialize(commandName)
                .serialize(transactionId)
                .serialize(commandObject)
            for i in arguments {
                serializer.serialize(i)
            }
            return serializer.data
        }
        set {
            if length == newValue.count {
                serializer.writeBytes(newValue)
                serializer.position = 0
                do {
                    if type == .amf3Command {
                        serializer.position = 1
                    }
                    commandName = try serializer.deserialize()
                    transactionId = try serializer.deserialize()
                    commandObject = try serializer.deserialize()
                    arguments.removeAll()
                    if 0 < serializer.bytesAvailable {
                        arguments.append(try serializer.deserialize())
                    }
                } catch {
                    logger.error("\(self.serializer)")
                }
                serializer.clear()
            }
        }
    }

    private var serializer: AMFSerializer = AMF0Serializer()

    init(objectEncoding: UInt8) {
        self.objectEncoding = objectEncoding
        self.type = objectEncoding == 0x00 ? .amf0Command : .amf3Command
    }

    init(streamId: UInt32, transactionId: Int, objectEncoding: UInt8, commandName: String, commandObject: ASObject?, arguments: [Any?]) {
        self.transactionId = transactionId
        self.objectEncoding = objectEncoding
        self.commandName = commandName
        self.commandObject = commandObject
        self.arguments = arguments
        self.type = objectEncoding == 0x00 ? .amf0Command : .amf3Command
        self.streamId = streamId
    }

    func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
        guard let responder: Responder = connection.operations.removeValue(forKey: transactionId) else {
            switch commandName {
            case "close":
                connection.close(isDisconnected: true)
            default:
                connection.dispatch(.rtmpStatus, bubbles: false, data: arguments.first)
            }
            return
        }

        switch commandName {
        case "_result":
            responder.on(result: arguments)
        case "_error":
            responder.on(status: arguments)
        default:
            break
        }
    }
}

// MARK: -
/**
 7.1.2. Data Message (18, 15)
 */
struct RTMPDataMessage: RTMPMessage {
    let type: RTMPMessageType
    var length: Int = 0
    var streamId: UInt32 = 0
    var timestamp: UInt32 = 0
    let objectEncoding: UInt8 = 0
    var handlerName: String = ""
    var arguments: [Any?] = []

    var payload: Data {
        get {
            defer {
                serializer.clear()
            }
            if type == .amf3Data {
                serializer.writeUInt8(0)
            }
            serializer.serialize(handlerName)
            for arg in arguments {
                serializer.serialize(arg)
            }

            return serializer.data
        }
        set {
            if length == newValue.count {
                serializer.writeBytes(newValue)
                serializer.position = 0
                if type == .amf3Data {
                    serializer.position = 1
                }
                do {
                    handlerName = try serializer.deserialize()
                    while 0 < serializer.bytesAvailable {
                        arguments.append(try serializer.deserialize())
                    }
                } catch {
                    logger.error("\(self.serializer)")
                }
                serializer.clear()
            }
        }
    }

    private var serializer: AMFSerializer = AMF0Serializer()

    init(objectEncoding: UInt8) {
        self.type = objectEncoding == 0x00 ? .amf0Data : .amf3Data
        self.objectEncoding = objectEncoding
    }

    init(streamId: UInt32, objectEncoding: UInt8, handlerName: String, arguments: [Any?] = []) {
        self.type = objectEncoding == 0x00 ? .amf0Data : .amf3Data
        self.objectEncoding = objectEncoding
        self.handlerName = handlerName
        self.arguments = arguments
        self.streamId = streamId
    }

    func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
        guard let stream: RTMPStream = connection.streams[streamId] else {
            return
        }
        OSAtomicAdd64(Int64(payload.count), &stream.info.byteCount)
    }
}

// MARK: -
/**
 7.1.3. Shared Object Message (19, 16)
 */
final class RTMPSharedObjectMessage: RTMPMessage {
    let objectEncoding: UInt8
    var sharedObjectName: String = ""
    var currentVersion: UInt32 = 0
    var flags = Data(count: 8)
    var events: [RTMPSharedObjectEvent] = []

    override var payload: Data {
        get {
            guard super.payload.isEmpty else {
                return super.payload
            }

            if type == .amf3Shared {
                serializer.writeUInt8(0)
            }

            serializer
                .writeUInt16(UInt16(sharedObjectName.utf8.count))
                .writeUTF8Bytes(sharedObjectName)
                .writeUInt32(currentVersion)
                .writeBytes(flags)
            for event in events {
                event.serialize(&serializer)
            }
            super.payload = serializer.data
            serializer.clear()

            return super.payload
        }
        set {
            if super.payload == newValue {
                return
            }

            if length == newValue.count {
                serializer.writeBytes(newValue)
                serializer.position = 0
                if type == .amf3Shared {
                    serializer.position = 1
                }
                do {
                    sharedObjectName = try serializer.readUTF8()
                    currentVersion = try serializer.readUInt32()
                    flags = try serializer.readBytes(8)
                    while 0 < serializer.bytesAvailable {
                        if let event: RTMPSharedObjectEvent = try RTMPSharedObjectEvent(serializer: &serializer) {
                            events.append(event)
                        }
                    }
                } catch {
                    logger.error("\(self.serializer)")
                }
                serializer.clear()
            }

            super.payload = newValue
        }
    }

    private var serializer: AMFSerializer = AMF0Serializer()

    init(objectEncoding: UInt8) {
        self.objectEncoding = objectEncoding
        super.init(type: objectEncoding == 0x00 ? .amf0Shared : .amf3Shared)
    }

    init(timestamp: UInt32, objectEncoding: UInt8, sharedObjectName: String, currentVersion: UInt32, flags: Data, events: [RTMPSharedObjectEvent]) {
        self.objectEncoding = objectEncoding
        self.sharedObjectName = sharedObjectName
        self.currentVersion = currentVersion
        self.flags = flags
        self.events = events
        super.init(type: objectEncoding == 0x00 ? .amf0Shared : .amf3Shared)
        self.timestamp = timestamp
    }

    override func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
        let persistence: Bool = (flags[3] & 2) != 0
        RTMPSharedObject.getRemote(withName: sharedObjectName, remotePath: connection.uri!.absoluteWithoutQueryString, persistence: persistence).on(message: self)
    }
}

// MARK: -
/**
 7.1.5. Audio Message (9)
 */
final class RTMPAudioMessage: RTMPMessage {
    private(set) var codec: FLVAudioCodec = .unknown
    private(set) var soundRate: FLVSoundRate = .kHz44
    private(set) var soundSize: FLVSoundSize = .snd8bit
    private(set) var soundType: FLVSoundType = .stereo

    override var payload: Data {
        get {
            return super.payload
        }
        set {
            if super.payload == newValue {
                return
            }

            super.payload = newValue

            if length == newValue.count && !newValue.isEmpty {
                guard let codec = FLVAudioCodec(rawValue: newValue[0] >> 4),
                    let soundRate = FLVSoundRate(rawValue: (newValue[0] & 0b00001100) >> 2),
                    let soundSize = FLVSoundSize(rawValue: (newValue[0] & 0b00000010) >> 1),
                    let soundType = FLVSoundType(rawValue: (newValue[0] & 0b00000001)) else {
                    return
                }
                self.codec = codec
                self.soundRate = soundRate
                self.soundSize = soundSize
                self.soundType = soundType
            }
        }
    }

    init() {
        super.init(type: .audio)
    }

    init(streamId: UInt32, timestamp: UInt32, payload: Data) {
        super.init(type: .audio)
        self.streamId = streamId
        self.timestamp = timestamp
        self.payload = payload
    }

    override func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
        guard let stream: RTMPStream = connection.streams[streamId] else {
            return
        }
        OSAtomicAdd64(Int64(payload.count), &stream.info.byteCount)
        guard codec.isSupported else {
            return
        }
        switch type {
        case .zero:
            stream.audioTimestamp = Double(timestamp)
        default:
            stream.audioTimestamp += Double(timestamp)
        }
        switch FLVAACPacketType(rawValue: payload[1]) {
        case .seq?:
            let config = AudioSpecificConfig(bytes: [UInt8](payload[codec.headerSize..<payload.count]))
            stream.mixer.audioIO.encoder.destination = .PCM
            stream.mixer.audioIO.encoder.inSourceFormat = config?.audioStreamBasicDescription()
        case .raw?:
            payload.withUnsafeMutableBytes { (buffer: UnsafeMutableRawBufferPointer) -> Void in
                stream.mixer.audioIO.encoder.encodeBytes(buffer.baseAddress?.advanced(by: codec.headerSize), count: payload.count - codec.headerSize, presentationTimeStamp: CMTime(seconds: stream.audioTimestamp / 1000, preferredTimescale: 1000))
            }
        case .none:
            break
        }
    }
}

// MARK: -
/**
 7.1.5. Video Message (9)
 */
final class RTMPVideoMessage: RTMPMessage {
    private(set) var codec: FLVVideoCodec = .unknown
    private(set) var status: OSStatus = noErr

    init() {
        super.init(type: .video)
    }

    init(streamId: UInt32, timestamp: UInt32, payload: Data) {
        super.init(type: .video)
        self.streamId = streamId
        self.timestamp = timestamp
        self.payload = payload
    }

    override func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
        guard let stream: RTMPStream = connection.streams[streamId] else {
            return
        }
        OSAtomicAdd64(Int64(payload.count), &stream.info.byteCount)
        guard FLVTagType.video.headerSize <= payload.count else {
            return
        }
        switch payload[1] {
        case FLVAVCPacketType.seq.rawValue:
            status = createFormatDescription(stream)
        case FLVAVCPacketType.nal.rawValue:
            enqueueSampleBuffer(stream, type: type)
        default:
            break
        }
    }

    func enqueueSampleBuffer(_ stream: RTMPStream, type: RTMPChunkType) {
        let compositionTimeoffset = Int32(data: [0] + payload[2..<5]).bigEndian

        var pts: CMTime = .invalid
        var dts: CMTime = .invalid
        switch type {
        case .zero:
            pts = CMTimeMake(value: Int64(timestamp) + Int64(compositionTimeoffset), timescale: 1000)
            dts = CMTimeMake(value: Int64(timestamp), timescale: 1000)
            stream.videoTimestamp = Double(dts.value)
        default:
            pts = CMTimeMake(value: Int64(stream.videoTimestamp) + Int64(timestamp) + Int64(compositionTimeoffset), timescale: 1000)
            dts = CMTimeMake(value: Int64(stream.videoTimestamp) + Int64(timestamp), timescale: 1000)
            stream.videoTimestamp = Double(dts.value)
        }

        var timing = CMSampleTimingInfo(
            duration: CMTimeMake(value: Int64(timestamp), timescale: 1000),
            presentationTimeStamp: pts,
            decodeTimeStamp: compositionTimeoffset == 0 ? CMTime.invalid : dts
        )

        payload.withUnsafeBytes { (buffer: UnsafeRawBufferPointer) -> Void in
            var blockBuffer: CMBlockBuffer?
            let length: Int = payload.count - FLVTagType.video.headerSize
            guard CMBlockBufferCreateWithMemoryBlock(
                allocator: kCFAllocatorDefault,
                memoryBlock: nil,
                blockLength: length,
                blockAllocator: nil,
                customBlockSource: nil,
                offsetToData: 0,
                dataLength: length,
                flags: 0,
                blockBufferOut: &blockBuffer) == noErr else {
                stream.mixer.videoIO.decoder.needsSync.mutate { $0 = true }
                return
            }
            guard CMBlockBufferReplaceDataBytes(
                with: buffer.baseAddress!.advanced(by: FLVTagType.video.headerSize),
                blockBuffer: blockBuffer!,
                offsetIntoDestination: 0,
                dataLength: length) == noErr else {
                return
            }
            var sampleBuffer: CMSampleBuffer?
            var sampleSizes: [Int] = [length]
            guard CMSampleBufferCreate(
                allocator: kCFAllocatorDefault,
                dataBuffer: blockBuffer!,
                dataReady: true,
                makeDataReadyCallback: nil,
                refcon: nil,
                formatDescription: stream.mixer.videoIO.formatDescription,
                sampleCount: 1,
                sampleTimingEntryCount: 1,
                sampleTimingArray: &timing,
                sampleSizeEntryCount: 1,
                sampleSizeArray: &sampleSizes,
                sampleBufferOut: &sampleBuffer) == noErr else {
                return
            }
            if let sampleBuffer = sampleBuffer {
                sampleBuffer.isNotSync = !(payload[0] >> 4 == FLVFrameType.key.rawValue)
                status = stream.mixer.videoIO.decoder.decodeSampleBuffer(sampleBuffer)
            }
            if stream.mixer.videoIO.queue.locked.value {
                stream.mixer.videoIO.queue.locked.mutate { value in
                    value = timestamp != 0
                }
            }
        }
    }

    func createFormatDescription(_ stream: RTMPStream) -> OSStatus {
        var config = AVCConfigurationRecord()
        config.data = payload.subdata(in: FLVTagType.video.headerSize..<payload.count)
        return config.createFormatDescription(&stream.mixer.videoIO.formatDescription)
    }
}

// MARK: -
/**
 7.1.6. Aggregate Message (22)
 */
struct RTMPAggregateMessage: RTMPMessage {
    let type: RTMPMessageType = .aggregate
    var length: Int = 0
    var streamId: UInt32 = 0
    var timestamp: UInt32 = 0
    var payload: Data = Data()
}

// MARK: -
/**
 7.1.7. User Control Message Events
 */
struct RTMPUserControlMessage: RTMPMessage {
    enum Event: UInt8 {
        case streamBegin = 0x00
        case streamEof = 0x01
        case streamDry = 0x02
        case setBuffer = 0x03
        case recorded = 0x04
        case ping = 0x06
        case pong = 0x07
        case bufferEmpty = 0x1F
        case bufferFull = 0x20
        case unknown = 0xFF

        var data: Data {
            return Data([0x00, rawValue])
        }
    }

    let type: RTMPMessageType = .user
    var length: Int = 0
    var streamId: UInt32 = 0
    var timestamp: UInt32 = 0
    var event: Event = .unknown
    var value: Int32 = 0

    var payload: Data {
        get {
            var data = Data()
            data.append(event.data)
            data.append(value.bigEndian.data)
            return data
        }
        set {
            if length == newValue.count {
                if let event = Event(rawValue: newValue[1]) {
                    self.event = event
                }
                value = Int32(data: newValue[2..<newValue.count]).bigEndian
            }
        }
    }

    init() {
    }

    init(event: Event, value: Int32) {
        self.event = event
        self.value = value
    }

    func execute(_ connection: RTMPConnection, type: RTMPChunkType) {
        switch event {
        case .ping:
            connection.socket.doOutput(chunk: RTMPChunk(
                type: .zero,
                streamId: RTMPChunk.StreamID.control.rawValue,
                message: RTMPUserControlMessage(event: .pong, value: value)
            ), locked: nil)
        case .bufferEmpty, .bufferFull:
            connection.streams[UInt32(value)]?.dispatch("rtmpStatus", bubbles: false, data: [
                "level": "status",
                "description": ""
            ])
        default:
            break
        }
    }
}

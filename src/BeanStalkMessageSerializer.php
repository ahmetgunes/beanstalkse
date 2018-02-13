<?php
/**
 * Created by A.
 * User: ahmetgunes
 * Date: 13.02.2018
 * Time: 16:17
 */

namespace BeanStalkEvent;


use ScheduledEvent\Model\Message\AbstractMessage;
use ScheduledEvent\Model\Message\MessageSerializerInterface;
use ScheduledEvent\Traits\ConvertibleTrait;

class BeanStalkMessageSerializer implements MessageSerializerInterface
{
    use ConvertibleTrait;

    public static function convert($message)
    {
        if ($message instanceof AbstractMessage) {
            $body = self::toArray($message);

            return [
                json_encode($body, true),
                $message->getPriority() ?: 0
            ];
        } else {
            throw new ScheduledEventException('Wrong message format passed to serializer');
        }
    }

    public static function deConvert($message)
    {
        if (is_array($message) && isset($message['body'])) {
            return self::toMessage($message['body']);
        } else {
            throw new ScheduledEventException('Empty job returned from the queue');
        }
    }

    protected static function toMessage(string $body)
    {
        $object = json_decode($body, true);
        $message = new BeanStalkMessage();
        foreach ($object as $key => $value) {
            $message->{$key} = $value;
        }
    }
}
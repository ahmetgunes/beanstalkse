<?php
/**
 * Created by A.
 * User: ahmetgunes
 * Date: 12.02.2018
 * Time: 23:51
 */

namespace BeanStalkEvent;

use PhpAmqpLib\Message\AMQPMessage;
use ScheduledEvent\Model\Exception\ScheduledEventException;
use ScheduledEvent\Model\Message\AbstractMessage;

class BeanStalkMessage extends AbstractMessage
{
    public function convert()
    {
        $body = $this->toArray();

        return [
            json_encode($body, true),
            $this->getPriority() ?: 0
        ];
    }

    public static function deConvert($message)
    {
        if (is_array($message) && isset($message['body'])) {
            $beanStalkMessage = new BeanStalkMessage();
            $beanStalkMessage->toObject($message['body']);

            return $beanStalkMessage;
        } else {
            throw new ScheduledEventException('Empty job returned from the queue');
        }
    }
}
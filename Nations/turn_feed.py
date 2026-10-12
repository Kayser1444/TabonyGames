import logging

from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from channels.layers import get_channel_layer
from django.conf import settings
from django.db import transaction

from .models import Match

logger = logging.getLogger(__name__)
TURN_FEED_GROUP = 'nations_pending_turns'


def pending_matches():
    # Read the stored turn owner, never replay the rules engine. No age cutoff:
    # an archived ongoing game still has a pending turn.
    return Match.objects.filter(game_over=False).exclude(
        replay=''
    ).exclude(current_player__username='deleted_player')


def publish_turn_change(match_id, using=None):
    def publish():
        try:
            async_to_sync(get_channel_layer().group_send)(TURN_FEED_GROUP, {
                'type': 'pending_turn_changed', 'game': str(match_id),
            })
        except Exception:
            # An optional notifier must not fail a player's successfully saved move.
            logger.exception('Could not publish Nations turn change for %s', match_id)

    transaction.on_commit(publish, using=using)


class NationsTurnFeedConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        self.joined = False
        if not getattr(settings, 'NATIONS_TURN_FEED_ENABLED', False):
            await self.close(code=1008)
            return
        await self.channel_layer.group_add(TURN_FEED_GROUP, self.channel_name)
        self.joined = True
        await self.accept()
        await self.send_json({'type': 'snapshot', 'turns': await self.get_snapshot()})

    async def disconnect(self, close_code):
        if self.joined:
            await self.channel_layer.group_discard(TURN_FEED_GROUP, self.channel_name)

    @database_sync_to_async
    def get_snapshot(self):
        return [{'game': str(game), 'player': player} for game, player in
                pending_matches().order_by('match_id').values_list(
                    'match_id', 'current_player__username')]

    @database_sync_to_async
    def get_player(self, game):
        return pending_matches().filter(pk=game).values_list(
            'current_player__username', flat=True).first()

    async def pending_turn_changed(self, event):
        # Resolve the latest committed owner when handling an event. In particular,
        # a queued pre-snapshot event cannot restore an obsolete pending player.
        await self.send_json({'type': 'turn', 'game': event['game'],
                              'player': await self.get_player(event['game'])})

    async def receive_json(self, content):
        if content is not None:
            await self.close(code=1008)
            return
        # A lightweight heartbeat also renews Redis group membership; no DB scan.
        await self.channel_layer.group_add(TURN_FEED_GROUP, self.channel_name)
        await self.send_json({'type': 'pong'})

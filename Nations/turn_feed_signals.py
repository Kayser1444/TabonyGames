from django.conf import settings
from django.db.models.signals import pre_save, post_save, post_delete
from django.dispatch import receiver

from .models import Match
from .turn_feed import publish_turn_change


@receiver(pre_save, sender=Match)
def remember_turn_owner(sender, instance, raw=False, using=None, **kwargs):
    if raw or not getattr(settings, 'NATIONS_TURN_FEED_ENABLED', False):
        return
    instance._previous_feed_turn = sender.objects.using(using).filter(
        pk=instance.pk).values_list('current_player_id', 'game_over').first()


@receiver(post_save, sender=Match)
def broadcast_turn_owner(sender, instance, raw=False, using=None, **kwargs):
    if raw or not getattr(settings, 'NATIONS_TURN_FEED_ENABLED', False):
        return
    before = getattr(instance, '_previous_feed_turn', None)
    after = (instance.current_player_id, instance.game_over)
    if before != after and (before is not None or instance.replay.strip()):
        publish_turn_change(instance.pk, using=using)


@receiver(post_delete, sender=Match)
def broadcast_removed_match(sender, instance, using=None, **kwargs):
    if getattr(settings, 'NATIONS_TURN_FEED_ENABLED', False):
        publish_turn_change(instance.pk, using=using)
